// app.js v2025-11-01-vercel-migration-bugfixes
(function(){
  const ENDPOINT = "https://vacancy.animeshkumar97.workers.dev";
  const qs=(s,r)=>(r||document).querySelector(s);
  const qsa=(s,r)=>Array.from((r||document).querySelectorAll(s));
  const esc=(s)=>(s==null?"":String(s)).replace(/[&<>\"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c]));
  const fmtDate=(s)=>s && s.toUpperCase()!=="N/A" ? s.replaceAll("-", "/") : "N/A";
  const bust=(p)=>p+(p.includes("?")?"&":"?")+"t="+Date.now();
  const toast=(m)=>{const t=qs("#toast"); if(!t) return alert(m); t.textContent=m; t.style.opacity="1"; clearTimeout(t._h); t._h=setTimeout(()=>t.style.opacity="0",1800); };

  function normHref(u){
    try{ const p=new URL(u.trim()); p.hash=""; p.search=""; let s=p.toString(); if(s.endsWith("/")) s=s.slice(0,-1); return s.toLowerCase(); }
    catch{ return (u||"").trim().toLowerCase().replace(/[?#].*$/,"").replace(/\/$/,""); }
  }

  async function renderStatus(){
    try{
      const r=await fetch(bust("health.json"),{cache:"no-store"}); if(!r.ok) throw 0;
      const h=await r.json();
      const pill=qs("#health-pill");
      pill.textContent=h.ok?"Health: OK":"Health: Not OK";
      pill.className="pill "+(h.ok?"ok":"bad");
      qs("#last-updated").textContent="Last updated: "+(h.lastUpdated? new Date(h.lastUpdated).toLocaleString() : "—");
      qs("#total-listings").textContent="Listings: "+(typeof h.totalListings==="number"?h.totalListings:"—");
    }catch{
      const pill=qs("#health-pill");
      pill.textContent="Health: Unknown";
      pill.className="pill";
      qs("#last-updated").textContent="Last updated: —";
      qs("#total-listings").textContent="Listings: —";
    }
  }

  document.addEventListener("click",(e)=>{ const t=e.target.closest(".tab"); if(!t) return;
    qsa(".tab").forEach(x=>x.classList.toggle("active",x===t));
    qsa(".panel").forEach(p=>p.classList.toggle("active", p.id==="panel-"+t.dataset.tab));
  });

  let USER_STATE={}, USER_VOTES={};
  const ACTIVE_TIMERS = new Map();

  async function loadUserStateServer(){
    try{
      const wr=await fetch(ENDPOINT+"?state=1",{mode:"cors"});
      if(wr.ok){
        const wj=await wr.json();
        if(wj && wj.ok){
          const serverVotes = (wj.votes && typeof wj.votes==="object") ? wj.votes : {};
          const localVotes = JSON.parse(localStorage.getItem("vac_user_votes")||"{}");
          
          USER_VOTES = {};
          const allJobIds = new Set([...Object.keys(serverVotes), ...Object.keys(localVotes)]);
          
          for(const jid of allJobIds) {
            const srv = serverVotes[jid];
            const loc = localVotes[jid];
            
            if(!loc) { USER_VOTES[jid] = srv; continue; }
            if(!srv) { USER_VOTES[jid] = loc; continue; }
            
            const srvTs = new Date(srv.ts || 0).getTime();
            const locTs = new Date(loc.ts || 0).getTime();
            USER_VOTES[jid] = (locTs >= srvTs) ? loc : srv;
          }
          
          try{ localStorage.setItem("vac_user_votes",JSON.stringify(USER_VOTES)); }catch{}
          
          if (wj.state && typeof wj.state==="object") USER_STATE={...wj.state};
          return;
        }
      }
    }catch(err){
      console.error("KV state fetch failed:", err);
    }
    try{
      const r=await fetch(bust("user_state.json"),{cache:"no-store"}); if(!r.ok) throw 0;
      const remote=await r.json(); if(remote && typeof remote==="object"){ USER_STATE={...remote}; }
    }catch{ USER_STATE={}; }
  }

  function loadUserStateLocal(){
    try{ const local=JSON.parse(localStorage.getItem("vac_user_state")||"{}"); if(local && typeof local==="object"){ USER_STATE={...USER_STATE, ...local}; } }catch{}
  }
  function loadVotesLocal(){ try{ USER_VOTES=JSON.parse(localStorage.getItem("vac_user_votes")||"{}")||{}; }catch{ USER_VOTES={}; } }

  function setUserStateLocal(id,a){
    if(!id) return;
    if(a==="undo") delete USER_STATE[id];
    else USER_STATE[id]={action:a,ts:new Date().toISOString()};
    try{ localStorage.setItem("vac_user_state",JSON.stringify(USER_STATE)); }catch{}
  }

  function setVoteLocal(id,v){ if(!id) return; if(v==="") delete USER_VOTES[id]; else USER_VOTES[id]={vote:v,ts:new Date().toISOString()}; try{ localStorage.setItem("vac_user_votes",JSON.stringify(USER_VOTES)); }catch{} }

  async function persistUserStateServer(){
    try{
      await fetch(ENDPOINT,{
        method:"POST",
        headers:{"Content-Type":"application/json"},
        body:JSON.stringify({ type:"user_state_sync", payload:USER_STATE, ts:new Date().toISOString() })
      });
    }catch(err){
      console.error("KV state sync failed:", err);
    }
  }

  function confirmAction(message="Proceed?"){
  return new Promise((resolve)=>{
    const modal=document.createElement("div");
    modal.className="confirm-modal";
    modal.innerHTML=`
      <div class="confirm-modal-overlay">
        <div class="confirm-modal-content">
          <h3>Vacancy Dashboard</h3>
          <p>${message}</p>
          <div class="confirm-modal-buttons">
            <button class="btn ghost cancel-btn">Cancel</button>
            <button class="btn primary confirm-btn">OK</button>
          </div>
        </div>
      </div>
    `;
    document.body.appendChild(modal);
    
    const cancel=modal.querySelector(".cancel-btn");
    const confirm_btn=modal.querySelector(".confirm-btn");
    
    cancel.onclick=()=>{
      modal.remove();
      resolve(false);
    };
    
    confirm_btn.onclick=()=>{
      modal.remove();
      resolve(true);
    };
    
    confirm_btn.focus();
  });
}

  const trustedChip=()=>' <span class="chip trusted">trusted</span>';
  const topVerify=()=>' <span class="verify-top" title="Verified Right">✓</span>';
  const corroboratedChip=()=>' <span class="chip" title="Multiple sources">x2</span>';

  function renderInlineUndo(slot, label, onUndo, onCommit, seconds=10){
    if(!slot) return;
    
    const cardId = slot.closest("[data-id]")?.getAttribute("data-id");
    
    if(cardId && ACTIVE_TIMERS.has(cardId)){
      clearInterval(ACTIVE_TIMERS.get(cardId));
      ACTIVE_TIMERS.delete(cardId);
    }
    
    const wrap=document.createElement("div");
    wrap.className="group";
    const b=document.createElement("button");
    b.className="btn ghost tiny";
    let left=seconds;
    
    const cleanup = () => {
      clearInterval(tick);
      if(cardId) ACTIVE_TIMERS.delete(cardId);
    };
    
    const tick=setInterval(()=>{ 
      left--; 
      if(left<=0){ 
        cleanup();
        b.disabled=true; 
        wrap.remove(); 
        onCommit(); 
      } else { 
        b.textContent=`Undo ${label} (${left}s)`; 
      } 
    },1000);
    
    if(cardId) ACTIVE_TIMERS.set(cardId, tick);
    
    b.textContent=`Undo ${label} (${left}s)`;
    b.onclick=(ev)=>{ 
      ev.preventDefault(); 
      cleanup();
      wrap.remove(); 
      onUndo(); 
    };
    
    wrap.appendChild(b);
    slot.replaceChildren(wrap);
  }

  function cardHTML(j, applied=false){
    const src=(j.source||"").toLowerCase()==="official" ? '<span class="chip" title="Official source">Official</span>' : '<span class="chip" title="From aggregator">Agg</span>';
    const d=(j.daysLeft!=null && j.daysLeft!=="")?j.daysLeft:"—";
    const det=esc(j.detailLink||j.applyLink||"#");
    const lid=j.id||"";
    const vote=USER_VOTES[lid]?.vote||"";
    const verified= vote==="right";
    const hasVoted = vote==="right" || vote==="wrong";
    const trust = j.flags && j.flags.trusted ? trustedChip() : "";
    const corr = j.flags && j.flags.corroborated ? corroboratedChip() : "";
    const tVerify = verified ? topVerify() : "";
    const appliedBadge = applied ? '<span class="badge-done">Applied</span>' : "";
    const posts = (j.numberOfPosts!=null && j.numberOfPosts!=="")?String(j.numberOfPosts):(j.flags && j.flags.posts ? String(j.flags.posts) : "N/A");

    let voteButtonsHTML = '';
    if (!hasVoted) {
      voteButtonsHTML = '<button class="vote-btn right" data-act="right" type="button">☑</button><button class="vote-btn wrong" data-act="wrong" type="button">☒</button>';
    }

    return [
      '<article class="card', (applied?' applied':''), (verified?' verified':''), '" data-id="', esc(lid), '">',
        '<header class="card-head"><h3 class="title">', esc(j.title||"No Title"), '</h3>', src, trust, corr, tVerify, appliedBadge, '</header>',
        '<div class="card-body">',
          '<div class="rowline"><span class="muted">Posts</span><span>', esc(posts), '</span></div>',
          '<div class="rowline"><span class="muted">Qualification</span><span>', esc(j.qualificationLevel||"N/A"), '</span></div>',
          '<div class="rowline"><span class="muted">Domicile</span><span>', esc(j.domicile||"All India"), '</span></div>',
          '<div class="rowline"><span class="muted">Last date</span><span>', esc(fmtDate(j.deadline)), ' <span class="muted">(', d, ' days)</span></span></div>',
        '</div>',
        '<div class="actions-row row1">',
          '<div class="left"><a class="btn primary" href="', det, '" target="_blank" rel="noopener">Details</a></div>',
          '<div class="right"><button class="btn danger" data-act="report" type="button">Report</button></div>',
        '</div>',
        '<div class="actions-row row2">',
          '<div class="group vote">',
            voteButtonsHTML,
          '</div>',
          '<div class="group interest">',
            applied
              ? '<button class="btn exam-done" data-act="exam_done" type="button">Exam done</button>'
              : '<button class="btn applied" data-act="applied" type="button">Applied</button><button class="btn other" data-act="not_interested" type="button">Not interested</button>',
          '</div>',
        '</div>',
      '</article>'
    ].join('');
  }

  function sortByDeadline(list){
    const parse=(s)=>{ if(!s||s.toUpperCase()==="N/A") return null;
      const a=s.replaceAll("-","/").split("/"); if(a.length!==3) return null;
      const ms=Date.UTC(+a[2],+a[1]-1,+a[0]); return isNaN(ms)?null:ms; };
    return list.slice().sort((a,b)=>{ const da=parse(a.deadline),db=parse(b.deadline);
      if(da===null&&db===null) return (a.title||"").localeCompare(b.title||"");
      if(da===null) return 1; if(db===null) return -1; return da-db; });
  }

  let TOKEN=0;

  async function render(){
    const my=++TOKEN;

    loadVotesLocal();
    loadUserStateLocal();

    let data=null;
    try{ const r=await fetch(bust("data.json"),{cache:"no-store"}); if(!r.ok) throw 0; data=await r.json(); }catch{ data=null; }
    if(my!==TOKEN) return;

    const rootOpen=qs("#open-root"), rootApp=qs("#applied-root"), rootOther=qs("#other-root");

    if(!data || !Array.isArray(data.jobListings)){
      rootOpen.innerHTML='<div class="empty">No active job listings found (data.json missing or invalid).</div>';
      rootApp.innerHTML='';
      rootOther.innerHTML='';
      return;
    }

    const list=sortByDeadline(data.jobListings||[]);
    const sections=data.sections||{};
    qs("#total-listings").textContent="Listings: "+list.length;

    const idsAppliedFromData=new Set(sections.applied||[]);
    const idsOtherFromData=new Set(sections.other||[]);

    const idsApplied=new Set(idsAppliedFromData), idsOther=new Set(idsOtherFromData);

    Object.entries(USER_STATE).forEach(([jid,s])=>{
      if(!s||!s.action)return;

      if(s.action==="applied"){
        idsApplied.add(jid);
        idsOther.delete(jid);
      }else if(s.action==="not_interested"){
        idsOther.add(jid);
        idsApplied.delete(jid);
      }else if(s.action==="undo"){
        idsApplied.delete(jid);
        idsOther.delete(jid);
      }else if(s.action==="exam_done"){
        const doneTs = new Date(s.ts);
        const now = new Date();
        const diffDays = (now - doneTs) / (1000*60*60*24);
        if(diffDays <= 7){
          idsApplied.add(jid);
          idsOther.delete(jid);
        }else{
          idsApplied.delete(jid);
          idsOther.delete(jid);
        }
      }
    });

    const fOpen=document.createDocumentFragment(), fApp=document.createDocumentFragment(), fOther=document.createDocumentFragment();

    for(const job of list){
      const id = job.id || "";
      const applied = idsApplied.has(id);
      const refused = idsOther.has(id);

      const wrap=document.createElement("div"); wrap.innerHTML=cardHTML(job, applied);
      const card=wrap.firstElementChild;

      card.addEventListener("click", async (e)=>{
        const btn=e.target.closest("[data-act]"); if(!btn) return;
        e.preventDefault(); e.stopPropagation();
        const act=btn.getAttribute("data-act"), id=card.getAttribute("data-id");
        const detailsUrl=(card.querySelector(".row1 .left a")?.href||"");
        const voteCell=card.querySelector(".row2 .vote");
        const interestCell=card.querySelector(".row2 .interest");

        if(act==="report"){
          const m=qs("#report-modal"); if(!m) return;
          const titleText = card.querySelector(".title")?.textContent?.trim() || "";
          qs("#reportListingId").value=id||"";
          qs("#reportListingTitle").value=titleText;
          qs("#reportListingUrl").value=detailsUrl;
          m.classList.remove("hidden"); m.setAttribute("aria-hidden","false"); m.style.display="flex";
          setTimeout(()=>qs("#reportReason")?.focus(),0);
          return;
        }

        if(act==="right"){
          const prev=USER_VOTES[id]?.vote||"";
          setVoteLocal(id,"right"); 
          card.classList.add("verified");
          
          renderInlineUndo(voteCell, "vote",
            async ()=>{ 
              if(prev==="right"){ setVoteLocal(id,""); } else { setVoteLocal(id,prev||""); }
              await fetch(ENDPOINT,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({type:"vote",vote:"undo_right",jobId:id,url:detailsUrl,ts:new Date().toISOString()})});
              await render(); 
            },
            async ()=>{ 
              await fetch(ENDPOINT,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({type:"vote",vote:"right",jobId:id,url:detailsUrl,ts:new Date().toISOString()})});
              await render(); 
            }, 10);
          return;
        }

        if(act==="wrong"){
          const prev=USER_VOTES[id]?.vote||"";
          setVoteLocal(id,"wrong");
          
          renderInlineUndo(voteCell, "vote",
            async ()=>{ 
              if(prev==="wrong"){ setVoteLocal(id,""); } else { setVoteLocal(id,prev||""); }
              await fetch(ENDPOINT,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({type:"vote",vote:"undo_wrong",jobId:id,url:detailsUrl,ts:new Date().toISOString()})});
              await render(); 
            },
            async ()=>{ 
              await fetch(ENDPOINT,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({type:"vote",vote:"wrong",jobId:id,url:detailsUrl,ts:new Date().toISOString()})});
              await render(); 
            }, 10);
          return;
        }

        if(act==="applied"||act==="not_interested"){
          const ok = await confirmAction(act==="applied" ? "Mark as Applied?" : "Move to Other (Not interested)?");
          if(!ok) return;
          const prev=USER_STATE[id]?.action||"";
          setUserStateLocal(id,act);
          renderInlineUndo(interestCell, act==="applied"?"applied":"choice",
            async ()=>{ if(prev){ setUserStateLocal(id,prev); } else { setUserStateLocal(id,"undo"); }
              await fetch(ENDPOINT,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({type:"state",payload:{jobId:id,action:"undo",ts:new Date().toISOString()}})});
              await render(); },
            async ()=>{ await fetch(ENDPOINT,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({type:"state",payload:{jobId:id,action:act,ts:new Date().toISOString()}})});
              await render(); }, 10);
          return;
        }

        if(act==="exam_done"){
          const prev = USER_STATE[id]?.action || "";
          setUserStateLocal(id, "exam_done");
          
          renderInlineUndo(interestCell, "exam done",
            async ()=> {
              if(prev){ setUserStateLocal(id, prev); } 
              else { setUserStateLocal(id, "undo"); }
              
              await fetch(ENDPOINT,{
                method:"POST",
                headers:{"Content-Type":"application/json"},
                body:JSON.stringify({
                  type:"state",
                  payload:{jobId:id, action:"undo", ts:new Date().toISOString()}
                })
              });
              await render();
            },
            async ()=> {
              await fetch(ENDPOINT,{
                method:"POST",
                headers:{"Content-Type":"application/json"},
                body:JSON.stringify({
                  type:"state",
                  payload:{jobId:id, action:"exam_done", ts:new Date().toISOString()}
                })
              });
              toast("Exam done marked - will auto-remove after 7 days");
              await render();
            }, 10);
          return;
        }
      });

      if(applied) fApp.appendChild(card);
      else if(refused) fOther.appendChild(card);
      else fOpen.appendChild(card);
    }

    rootOpen.replaceChildren(fOpen);
    rootApp.replaceChildren(fApp);
    rootOther.replaceChildren(fOther);
  }

  function openModal(sel){
    const m=qs(sel); if(!m) return;
    m.classList.remove("hidden"); m.setAttribute("aria-hidden","false"); m.style.display="flex";
    if(sel==="#missing-modal"){ setTimeout(()=>qs("#missingTitle")?.focus(),0); }
  }
  function closeModalEl(el){
    const m=el.closest(".modal"); if(m){ m.classList.add("hidden"); m.setAttribute("aria-hidden","true"); m.style.display="none"; }
  }
  document.addEventListener("click",(e)=>{
    if(e.target && (e.target.hasAttribute("data-close") || e.target.classList.contains("close-top"))){ e.preventDefault(); closeModalEl(e.target); }
    if(e.target && e.target.classList.contains("modal")){ e.preventDefault(); e.target.classList.add("hidden"); e.target.setAttribute("aria-hidden","true"); e.target.style.display="none"; }
  });

  const reportForm = document.getElementById("reportForm");
  if(reportForm){
    reportForm.addEventListener("submit", async (e)=>{
      e.preventDefault();
      const reasonCode = qs("#reportReason")?.value?.trim();
      if(!reasonCode){
        toast("Please select a reason");
        return;
      }
      const payload = {
        type: "report",
        jobId: qs("#reportListingId")?.value?.trim() || "",
        title: qs("#reportListingTitle")?.value?.trim() || "",
        url: qs("#reportListingUrl")?.value?.trim() || "",
        reasonCode: reasonCode,
        evidenceUrl: qs("#reportEvidenceUrl")?.value?.trim() || "",
        posts: qs("#reportPosts")?.value?.trim() || "",
        lastDate: qs("#reportLastDate")?.value?.trim() || "",
        eligibility: qs("#reportEligibility")?.value?.trim() || "",
        note: qs("#reportNote")?.value?.trim() || "",
        ts: new Date().toISOString()
      };
      try{
        const res = await fetch(ENDPOINT, {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify(payload)
        });
        const json = await res.json();
        if(res.ok && json.ok){
          toast("Report submitted");
          closeModalEl(reportForm);
          reportForm.reset();
        } else {
          toast("Report failed: " + (json.error || "unknown"));
          console.error("Report error:", json);
        }
      }catch(err){
        toast("Report failed: network error");
        console.error("Report fetch error:", err);
      }
    });
  }

  const missingForm = document.getElementById("missingForm");
  if(missingForm){
    missingForm.addEventListener("submit", async (e)=>{
      e.preventDefault();
      const title = qs("#missingTitle")?.value?.trim();
      const url = qs("#missingUrl")?.value?.trim();
      if(!title || !url){
        toast("Title and URL are required");
        return;
      }
      const payload = {
        type: "missing",
        title: title,
        url: url,
        officialSite: qs("#missingSite")?.value?.trim() || "",
        posts: qs("#missingPosts")?.value?.trim() || "",
        lastDate: qs("#missingLastDate")?.value?.trim() || "",
        note: qs("#missingNote")?.value?.trim() || "",
        ts: new Date().toISOString()
      };
      try{
        const res = await fetch(ENDPOINT, {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify(payload)
        });
        const json = await res.json();
        if(res.ok && json.ok){
          toast("Submission received");
          closeModalEl(missingForm);
          missingForm.reset();
        } else {
          toast("Submission failed: " + (json.error || "unknown"));
          console.error("Missing submission error:", json);
        }
      }catch(err){
        toast("Submission failed: network error");
        console.error("Missing fetch error:", err);
      }
    });
  }

  const btnMissing = document.getElementById("btn-missing");
  if(btnMissing){
    btnMissing.addEventListener("click", ()=>openModal("#missing-modal"));
  }

  document.addEventListener("DOMContentLoaded", async ()=>{
    await loadUserStateServer();
    await renderStatus();
    await render();
  });
})();
