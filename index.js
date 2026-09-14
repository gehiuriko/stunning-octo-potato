import { AwsClient } from "aws4fetch";

const VERSION = "3.1.0-b2";
const DEFAULT_MAX = 1024 * 1024 * 1024;
const MULTIPART_THRESHOLD = 64 * 1024 * 1024;
const PART_SIZE = 16 * 1024 * 1024;
const CONCURRENCY = 4;

function allowedOrigin(request, env) {
  const origin = request.headers.get("Origin") || "";
  const allowed = String(env.FRONTEND_ORIGINS || "").split(",").map(x => x.trim()).filter(Boolean);
  return allowed.includes(origin) ? origin : "";
}
function cors(request, env) {
  const origin = allowedOrigin(request, env);
  return origin ? {
    "Access-Control-Allow-Origin": origin,
    "Access-Control-Allow-Methods": "GET,POST,DELETE,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type,X-Builder-Key",
    "Access-Control-Max-Age": "86400",
    "Vary": "Origin"
  } : {};
}
function json(request, env, data, status=200) {
  return new Response(JSON.stringify(data, null, 2), {status, headers:{"Content-Type":"application/json; charset=utf-8", ...cors(request,env)}});
}
function fail(request, env, message, status=400) { return json(request, env, {error:message}, status); }
function requireAuth(request, env) {
  const expected=String(env.BUILDER_KEY||"");
  const got=request.headers.get("X-Builder-Key")||"";
  if(!expected || got!==expected) throw new Response(JSON.stringify({error:"Builder Key salah."}), {status:401,headers:{"Content-Type":"application/json",...cors(request,env)}});
}
function makeId(){const a=new Uint32Array(2);crypto.getRandomValues(a);return `job-${Date.now()}-${a[0].toString(36)}${a[1].toString(36)}`;}
function safeId(v){return /^job-[0-9]+-[a-z0-9]+$/i.test(v||"");}
function encodeKey(key){return String(key).split("/").map(encodeURIComponent).join("/");}
function xmlEscape(s){return String(s).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&apos;"}[c]));}
function bodyJson(request){return request.json().catch(()=>({}));}

function s3(env){
  return new AwsClient({
    service:"s3",
    region:String(env.B2_REGION||""),
    accessKeyId:env.B2_KEY_ID,
    secretAccessKey:env.B2_APPLICATION_KEY
  });
}
function endpoint(env){
  const region=String(env.B2_REGION||"").trim();
  if(!region) throw new Error("B2_REGION belum diatur.");
  return `https://s3.${region}.backblazeb2.com`;
}
function objectUrl(env,key){
  return `${endpoint(env)}/${encodeURIComponent(env.B2_BUCKET_NAME)}/${encodeKey(key)}`;
}
async function signedUrl(env,key,method="GET",expires=900,headers={}){
  const u=new URL(objectUrl(env,key));
  u.searchParams.set("X-Amz-Expires",String(expires));
  const req=new Request(u.toString(),{method,headers});
  return (await s3(env).sign(req,{aws:{signQuery:true}})).url.toString();
}
async function b2Fetch(env,key,{method="GET",headers={},body=null,query=null}={}){
  const u=new URL(objectUrl(env,key));
  if(query) for(const [k,v] of Object.entries(query)) u.searchParams.set(k,v);
  return s3(env).fetch(new Request(u.toString(),{method,headers,body}));
}
async function loadStatus(env,id){
  const res=await b2Fetch(env,`jobs/${id}/status.json`);
  if(res.status===404) return null;
  if(!res.ok) throw new Error(`B2 status GET ${res.status}: ${(await res.text()).slice(0,500)}`);
  return JSON.parse(await res.text());
}
async function saveStatus(env,id,data){
  const text=JSON.stringify(data,null,2);
  const res=await b2Fetch(env,`jobs/${id}/status.json`,{method:"PUT",headers:{"Content-Type":"application/json"},body:text});
  if(!res.ok) throw new Error(`B2 status PUT ${res.status}: ${(await res.text()).slice(0,500)}`);
  return data;
}
async function headObject(env,key){
  const res=await b2Fetch(env,key,{method:"HEAD"});
  if(res.status===404) return false;
  if(!res.ok) throw new Error(`B2 HEAD ${res.status}`);
  return true;
}
async function deleteObject(env,key){
  if(!key) return;
  const res=await b2Fetch(env,key,{method:"DELETE"});
  if(!res.ok && res.status!==404) throw new Error(`B2 DELETE ${res.status}: ${(await res.text()).slice(0,300)}`);
}
async function createMultipart(env,key){
  const res=await b2Fetch(env,key,{method:"POST",query:{uploads:""}});
  const text=await res.text();
  if(!res.ok) throw new Error(`B2 create multipart ${res.status}: ${text.slice(0,500)}`);
  const m=text.match(/<UploadId>([^<]+)<\/UploadId>/);
  if(!m) throw new Error("B2 tidak mengembalikan UploadId.");
  return m[1];
}
async function completeMultipart(env,key,uploadId,parts){
  const xml=`<CompleteMultipartUpload>${parts.map(p=>`<Part><PartNumber>${Number(p.partNumber)}</PartNumber><ETag>${xmlEscape(p.etag)}</ETag></Part>`).join("")}</CompleteMultipartUpload>`;
  const res=await b2Fetch(env,key,{method:"POST",query:{uploadId},headers:{"Content-Type":"application/xml"},body:xml});
  const text=await res.text();
  if(!res.ok) throw new Error(`B2 complete multipart ${res.status}: ${text.slice(0,500)}`);
  return text;
}
async function abortMultipart(env,key,uploadId){
  if(!uploadId) return;
  const res=await b2Fetch(env,key,{method:"DELETE",query:{uploadId}});
  if(!res.ok && res.status!==404) throw new Error(`B2 abort multipart ${res.status}`);
}
async function dispatch(env,id,sourceKey,variant){
  const url=`https://api.github.com/repos/${encodeURIComponent(env.GITHUB_OWNER)}/${encodeURIComponent(env.GITHUB_REPO)}/actions/workflows/build-apk.yml/dispatches`;
  const res=await fetch(url,{method:"POST",headers:{
    "Accept":"application/vnd.github+json",
    "Authorization":`Bearer ${env.GITHUB_TOKEN}`,
    "X-GitHub-Api-Version":"2022-11-28",
    "User-Agent":"android-builder-turbo-b2-worker"
  },body:JSON.stringify({ref:env.GITHUB_BRANCH||"main",inputs:{job_id:id,source_key:sourceKey,variant}})});
  const text=await res.text();
  if(!res.ok) throw new Error(`GitHub ${res.status}: ${text.slice(0,800)}`);
  return {};
}

async function route(request,env){
  const url=new URL(request.url), p=url.pathname;
  if(request.method==="OPTIONS") return new Response(null,{status:204,headers:cors(request,env)});
  if(p==="/api/health"&&request.method==="GET"){
    requireAuth(request,env);
    const probe=await b2Fetch(env,"__builder_health_probe__",{method:"HEAD"});
    if(![200,404].includes(probe.status)) return fail(request,env,`B2 belum terhubung (HTTP ${probe.status}).`,502);
    return json(request,env,{ok:true,version:VERSION,engine:`${env.GITHUB_OWNER}/${env.GITHUB_REPO}`,storage:`Backblaze B2 · ${env.B2_REGION}`});
  }
  requireAuth(request,env);

  if(p==="/api/jobs"&&request.method==="POST"){
    const b=await bodyJson(request), fileName=String(b.fileName||""), size=Number(b.size||0), max=Number(env.MAX_UPLOAD_BYTES||DEFAULT_MAX);
    if(!fileName.toLowerCase().endsWith(".zip")) return fail(request,env,"File harus .zip");
    if(!Number.isFinite(size)||size<=0||size>max) return fail(request,env,`Ukuran ZIP tidak valid atau melebihi ${max} byte.`);
    const id=makeId(), sourceKey=`jobs/${id}/source.zip`, mode=size>=MULTIPART_THRESHOLD?"multipart":"single";
    const status={state:"created",job_id:id,file_name:fileName,size,source_key:sourceKey,mode,created_at:new Date().toISOString(),message:"Menunggu upload project."};
    if(mode==="multipart"){
      const uploadId=await createMultipart(env,sourceKey); status.upload_id=uploadId; await saveStatus(env,id,status);
      return json(request,env,{jobId:id,mode,uploadId,partSize:PART_SIZE,concurrency:CONCURRENCY});
    }
    await saveStatus(env,id,status);
    const contentType="application/zip";
    const uploadUrl=await signedUrl(env,sourceKey,"PUT",3600,{"Content-Type":contentType});
    return json(request,env,{jobId:id,mode,uploadUrl,uploadHeaders:{"Content-Type":contentType}});
  }

  let m=p.match(/^\/api\/jobs\/([^/]+)\/part-url$/);
  if(m&&request.method==="POST"){
    const id=m[1]; if(!safeId(id)) return fail(request,env,"Job ID invalid.");
    const st=await loadStatus(env,id); if(!st) return fail(request,env,"Job tidak ditemukan.",404);
    const b=await bodyJson(request); if(b.uploadId!==st.upload_id) return fail(request,env,"Upload ID tidak cocok.",409);
    const n=Number(b.partNumber); if(!Number.isInteger(n)||n<1||n>10000) return fail(request,env,"Part number invalid.");
    const u=new URL(objectUrl(env,st.source_key)); u.searchParams.set("partNumber",String(n)); u.searchParams.set("uploadId",b.uploadId); u.searchParams.set("X-Amz-Expires","3600");
    const signed=(await s3(env).sign(new Request(u.toString(),{method:"PUT"}),{aws:{signQuery:true}})).url.toString();
    return json(request,env,{url:signed});
  }

  m=p.match(/^\/api\/jobs\/([^/]+)\/complete-upload$/);
  if(m&&request.method==="POST"){
    const id=m[1]; if(!safeId(id)) return fail(request,env,"Job ID invalid.");
    const st=await loadStatus(env,id); if(!st) return fail(request,env,"Job tidak ditemukan.",404);
    const b=await bodyJson(request); if(b.uploadId!==st.upload_id||!Array.isArray(b.parts)||!b.parts.length) return fail(request,env,"Data multipart tidak valid.");
    const parts=[...b.parts].sort((a,b)=>a.partNumber-b.partNumber);
    if(parts.some(x=>!Number.isInteger(Number(x.partNumber))||!String(x.etag||"").trim())) return fail(request,env,"ETag multipart tidak lengkap.");
    await completeMultipart(env,st.source_key,b.uploadId,parts);
    st.state="uploaded"; st.message="Upload project selesai."; st.uploaded_at=new Date().toISOString(); await saveStatus(env,id,st);
    return json(request,env,{ok:true});
  }

  m=p.match(/^\/api\/jobs\/([^/]+)\/start$/);
  if(m&&request.method==="POST"){
    const id=m[1]; if(!safeId(id)) return fail(request,env,"Job ID invalid.");
    const st=await loadStatus(env,id); if(!st) return fail(request,env,"Job tidak ditemukan.",404);
    if(!(await headObject(env,st.source_key))) return fail(request,env,"Source ZIP belum selesai diupload.",409);
    const b=await bodyJson(request), variant=b.variant==="release"?"release":"debug";
    st.state="dispatching"; st.variant=variant; st.message="Memicu GitHub Runner…"; await saveStatus(env,id,st);
    try{await dispatch(env,id,st.source_key,variant);st.state="queued";st.message="Menunggu GitHub Runner.";await saveStatus(env,id,st);return json(request,env,{ok:true});}
    catch(e){st.state="failure";st.message=e.message;await saveStatus(env,id,st);throw e;}
  }

  m=p.match(/^\/api\/jobs\/([^/]+)\/download$/);
  if(m&&request.method==="POST"){
    const id=m[1]; if(!safeId(id)) return fail(request,env,"Job ID invalid.");
    const st=await loadStatus(env,id); if(!st) return fail(request,env,"Job tidak ditemukan.",404);
    const b=await bodyJson(request); let key,name;
    if(b.kind==="log"){key=st.log_key;name="build.log";}
    else{const f=(st.files||[]).find(x=>x.name===b.name)||(st.files||[])[0];if(f){key=f.key;name=f.name;}}
    if(!key) return fail(request,env,"File belum tersedia.",404);
    const dl=await signedUrl(env,key,"GET",900);
    return json(request,env,{url:dl,name,expiresIn:900});
  }

  m=p.match(/^\/api\/jobs\/([^/]+)$/);
  if(m&&request.method==="GET"){
    const id=m[1]; if(!safeId(id)) return fail(request,env,"Job ID invalid.");
    const st=await loadStatus(env,id); if(!st) return fail(request,env,"Job tidak ditemukan.",404);
    return json(request,env,st);
  }
  if(m&&request.method==="DELETE"){
    const id=m[1]; if(!safeId(id)) return fail(request,env,"Job ID invalid.");
    const st=await loadStatus(env,id);
    if(st){
      if(st.upload_id && !(await headObject(env,st.source_key))) { try{await abortMultipart(env,st.source_key,st.upload_id);}catch{} }
      const keys=[st.source_key,st.log_key,...(st.files||[]).map(x=>x.key),`jobs/${id}/status.json`].filter(Boolean);
      for(const key of keys){try{await deleteObject(env,key);}catch(e){console.warn("cleanup",key,e.message)}}
    }
    return json(request,env,{ok:true});
  }
  return fail(request,env,"Endpoint tidak ditemukan.",404);
}

export default {
  async fetch(request,env){
    try{return await route(request,env)}
    catch(e){if(e instanceof Response)return e;console.error(e);return fail(request,env,e?.message||"Internal error",500)}
  }
};
