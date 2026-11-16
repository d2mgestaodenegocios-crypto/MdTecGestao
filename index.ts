// ════════════════════════════════════════════════════════════════════
//  MdTec Gestão — Robô do Instagram (Supabase Edge Function "instagram")
//
//  Rotas (todas em https://<projeto>.supabase.co/functions/v1/instagram):
//    GET  /callback          → retorno do "Conectar com Instagram" (OAuth)
//    GET  /midia/<id>.jpg    → entrega a mídia do post para o Instagram baixar (link assinado)
//    POST /cron              → roda a cada 5 min (pg_cron): publica posts agendados e renova acessos
//    POST /api               → ações do sistema (usuário logado): estado, link-conexao, publicar,
//                              desconectar, testar
//
//  Segredos (Edge Functions → Secrets):
//    IG_APP_ID, IG_APP_SECRET   → do app da Meta (Instagram API com login do Instagram)
//    APP_URL                    → endereço do sistema (ex.: https://md-tec-gestao.vercel.app)
//    IG_SIGN_SECRET             → texto aleatório (assina links de conexão e de mídia)
//    CRON_SECRET                → texto aleatório (protege a rota /cron)
//  SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY já existem automaticamente.
//
//  IMPORTANTE: desligue "Verify JWT" nesta função (o Instagram precisa acessar /callback e /midia).
//  As ações de /api conferem o login do usuário dentro do código.
// ════════════════════════════════════════════════════════════════════
import { createClient } from "npm:@supabase/supabase-js@2";

const env = (k: string) => (Deno.env.get(k) || "").trim();
const SUPABASE_URL = env("SUPABASE_URL");
const SERVICE_KEY = env("SUPABASE_SERVICE_ROLE_KEY");
const IG_APP_ID = env("IG_APP_ID");
const IG_APP_SECRET = env("IG_APP_SECRET");
const APP_URL = env("APP_URL").replace(/\/+$/, "");
const SIGN_SECRET = env("IG_SIGN_SECRET") || SERVICE_KEY;
const CRON_SECRET = env("CRON_SECRET");

const GRAPH = "https://graph.instagram.com/v25.0";
const FN_URL = SUPABASE_URL + "/functions/v1/instagram";
const REDIRECT_URI = FN_URL + "/callback";
const ROW_ID = 24; // linha do módulo Instagram no app_data
const SCOPES = "instagram_business_basic,instagram_business_content_publish";

const db = createClient(SUPABASE_URL, SERVICE_KEY, { auth: { persistSession: false } });

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-cron-secret",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
};
const json = (obj: unknown, status = 200) =>
  new Response(JSON.stringify(obj), { status, headers: { ...CORS, "Content-Type": "application/json" } });
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// ── Assinatura (HMAC) ────────────────────────────────────────────────
const enc = new TextEncoder();
let _key: CryptoKey | null = null;
async function hmacKey() {
  if (!_key) _key = await crypto.subtle.importKey("raw", enc.encode(SIGN_SECRET), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  return _key;
}
async function assinar(txt: string) {
  const sig = new Uint8Array(await crypto.subtle.sign("HMAC", await hmacKey(), enc.encode(txt)));
  return Array.from(sig).map((b) => b.toString(16).padStart(2, "0")).join("").slice(0, 40);
}
function iguais(a: string, b: string) {
  if (a.length !== b.length) return false;
  let r = 0;
  for (let i = 0; i < a.length; i++) r |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return r === 0;
}
const b64url = (s: string) => btoa(String.fromCharCode(...enc.encode(s))).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
const deb64url = (s: string) => new TextDecoder().decode(Uint8Array.from(atob(s.replace(/-/g, "+").replace(/_/g, "/")), (c) => c.charCodeAt(0)));

// ── Leitura/gravação segura da linha 24 (mesma regra do sistema: compara _savedAt) ──
// deno-lint-ignore no-explicit-any
type Payload = any;
async function lerPayload(): Promise<Payload> {
  const { data, error } = await db.from("app_data").select("payload").eq("id", ROW_ID).maybeSingle();
  if (error) throw error;
  return data?.payload || { contas: [], posts: [] };
}
async function gravar(mut: (p: Payload) => Payload): Promise<Payload> {
  for (let t = 0; t < 8; t++) {
    const { data: row, error } = await db.from("app_data").select("payload").eq("id", ROW_ID).maybeSingle();
    if (error) throw error;
    const remoto = row?.payload || null;
    const base = remoto ? structuredClone(remoto) : { contas: [], posts: [] };
    if (!Array.isArray(base.contas)) base.contas = [];
    if (!Array.isArray(base.posts)) base.posts = [];
    const final = mut(base);
    const ts = Math.max(Date.now(), ((remoto && remoto._savedAt) || 0) + 1);
    final._savedAt = ts;
    final._t = ts;
    if (!remoto || remoto._savedAt == null) {
      const { error: e } = await db.from("app_data").upsert({ id: ROW_ID, payload: final });
      if (e) throw e;
      return final;
    }
    const { data: upd, error: e2 } = await db.from("app_data").update({ payload: final })
      .eq("id", ROW_ID).eq("payload->>_savedAt", String(remoto._savedAt)).select("id");
    if (e2) throw e2;
    if (upd && upd.length > 0) return final;
    await sleep(150 + Math.random() * 300);
  }
  throw new Error("Muitas gravações ao mesmo tempo — tente de novo.");
}

// ── Tokens (tabela ig_tokens, só a função acessa) ────────────────────
async function tokenDaConta(contaId: string) {
  const { data, error } = await db.from("ig_tokens").select("*").eq("conta_id", contaId).maybeSingle();
  if (error) throw new Error("Tabela ig_tokens não encontrada — rode o SQL de instalação. (" + error.message + ")");
  return data as null | { conta_id: string; ig_user_id: string; username: string; access_token: string; expires_at: string; updated_at: string };
}

// ── Chamadas à API do Instagram ──────────────────────────────────────
function mensagemErro(j: Payload, status: number) {
  const e = j?.error || {};
  const code = e.code, sub = e.error_subcode;
  if (code === 190) return "A conexão com o Instagram expirou ou foi removida. Reconecte a conta.";
  if (code === 4 || code === 17 || code === 32 || code === 613) return "Limite de uso da API do Instagram atingido. Tente mais tarde.";
  if (code === 9 || sub === 2207042) return "Limite de 100 publicações por dia atingido nesta conta.";
  if (code === 10 || code === 200) return "Permissão negada pelo Instagram. Confira se a conta é profissional e se o app tem a permissão de publicar.";
  if (sub === 2207026 || sub === 2207004) return "Formato da mídia não aceito pelo Instagram (" + (e.error_user_msg || e.message || "") + ").";
  if (sub === 2207052 || sub === 2207003) return "O Instagram não conseguiu baixar a mídia. Tente de novo em alguns minutos.";
  return (e.error_user_msg || e.message || ("Erro " + status)) + (e.code ? " [código " + e.code + (sub ? "/" + sub : "") + "]" : "");
}
async function gPost(path: string, body: Record<string, unknown>, token: string) {
  const r = await fetch(GRAPH + path, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: "Bearer " + token },
    body: JSON.stringify(body),
  });
  const j = await r.json().catch(() => ({}));
  if (!r.ok || j.error) throw new Error(mensagemErro(j, r.status));
  return j;
}
async function gGet(path: string, token: string) {
  const r = await fetch(GRAPH + path, { headers: { Authorization: "Bearer " + token } });
  const j = await r.json().catch(() => ({}));
  if (!r.ok || j.error) throw new Error(mensagemErro(j, r.status));
  return j;
}

// ── Mídia: link público e assinado para o Instagram baixar ───────────
async function urlMidia(m: Payload) {
  if (m.url) return m.url as string; // já está no Storage público
  const ext = m.tipo === "video" ? "mp4" : "jpg";
  return `${FN_URL}/midia/${m.id}.${ext}?s=${await assinar("midia:" + m.id)}`;
}
async function servirMidia(url: URL) {
  const m = url.pathname.match(/\/midia\/(\d+)(?:\.\w+)?$/);
  if (!m) return new Response("not found", { status: 404 });
  const id = m[1];
  const s = url.searchParams.get("s") || "";
  if (!iguais(s, await assinar("midia:" + id))) return new Response("forbidden", { status: 403 });
  const { data } = await db.from("app_data").select("payload").eq("id", Number(id)).maybeSingle();
  const p = data?.payload;
  if (!p || p.tipo !== "ig-midia" || typeof p.data !== "string") return new Response("not found", { status: 404 });
  const mt = p.data.match(/^data:([^;]+);base64,(.*)$/s);
  if (!mt) return new Response("bad media", { status: 500 });
  const bin = Uint8Array.from(atob(mt[2]), (c) => c.charCodeAt(0));
  return new Response(bin, {
    headers: { "Content-Type": mt[1] || "image/jpeg", "Content-Length": String(bin.length), "Cache-Control": "public, max-age=3600" },
  });
}

// ── Momento do post (data/hora do Brasil) ────────────────────────────
function quandoPost(p: Payload): number {
  if (p.quando) return Date.parse(p.quando);
  if (!p.data) return Infinity;
  return Date.parse(`${p.data}T${p.hora || "09:00"}:00-03:00`);
}
const legendaDe = (p: Payload) => [String(p.legenda || "").trim(), String(p.hashtags || "").trim()].filter(Boolean).join("\n\n");

// ── Publicação ───────────────────────────────────────────────────────
// Etapas retomáveis: cria os contêineres, espera o Instagram processar, publica.
// Se um vídeo demorar, salva o estado ("processando") e o cron continua na próxima rodada.
async function publicarPost(postId: string, porQuem: string, orcamentoMs = 100000): Promise<Payload> {
  const t0 = Date.now();
  let post: Payload = null, conta: Payload = null, travou = false, motivo = "";
  await gravar((d) => {
    travou = false;
    const x = d.posts.find((p: Payload) => p.id === postId);
    if (!x) { motivo = "Post não encontrado."; return d; }
    if (x.status === "publicado") { motivo = "Este post já foi publicado."; return d; }
    const r = x.robo || {};
    if (r.estado === "publicando" && Date.now() - Date.parse(r.desde || "0") < 4 * 60000) { motivo = "Este post já está sendo publicado agora."; return d; }
    x.robo = { ...r, estado: "publicando", desde: new Date().toISOString(), erro: "", por: porQuem };
    post = structuredClone(x);
    conta = d.contas.find((c: Payload) => c.id === x.contaId) || null;
    travou = true;
    return d;
  });
  if (!travou) return { ok: false, erro: motivo };

  const salvarRobo = (robo: Payload, extra: Payload = {}) => gravar((d) => {
    const x = d.posts.find((p: Payload) => p.id === postId);
    if (x) { x.robo = { ...(x.robo || {}), ...robo }; Object.assign(x, extra); }
    return d;
  });

  try {
    if (!conta) throw new Error("A conta deste post não existe mais.");
    const tk = await tokenDaConta(conta.id);
    if (!tk) throw new Error("A conta @" + (conta.usuario || conta.nome) + " não está conectada ao Instagram.");
    const token = tk.access_token, ig = tk.ig_user_id;
    const midias: Payload[] = post.midias || [];
    if (!midias.length) throw new Error("O post não tem imagem nem vídeo.");
    const legenda = legendaDe(post);
    if (legenda.length > 2200) throw new Error("A legenda passou de 2.200 caracteres.");
    let robo = post.robo || {};

    const esperar = async (id: string) => {
      while (true) {
        const j = await gGet(`/${id}?fields=status_code,status`, token);
        if (j.status_code === "FINISHED" || j.status_code === "PUBLISHED") return "OK";
        if (j.status_code === "ERROR" || j.status_code === "EXPIRED") {
          throw new Error(j.status_code === "EXPIRED" ? "O processamento expirou no Instagram (mais de 24h). Tente publicar de novo." : "O Instagram recusou a mídia: " + (j.status || "erro no processamento") + ".");
        }
        if (Date.now() - t0 > orcamentoMs) return "AGUARDE";
        await sleep(5000);
      }
    };

    if (!robo.containerId) {
      if (post.tipo === "carrossel" && midias.length > 1) {
        let filhos: string[] = Array.isArray(robo.filhos) ? robo.filhos : [];
        if (filhos.length !== midias.length) {
          filhos = [];
          for (const m of midias.slice(0, 10)) {
            const body = m.tipo === "video"
              ? { media_type: "VIDEO", video_url: await urlMidia(m), is_carousel_item: true }
              : { image_url: await urlMidia(m), is_carousel_item: true };
            filhos.push((await gPost(`/${ig}/media`, body, token)).id);
          }
          robo = { ...robo, filhos };
          await salvarRobo({ filhos });
        }
        for (const f of filhos) {
          if ((await esperar(f)) === "AGUARDE") {
            await salvarRobo({ estado: "processando", filhos });
            return { ok: true, pendente: true, msg: "O Instagram ainda está processando o vídeo. O robô termina a publicação em alguns minutos." };
          }
        }
        const c = await gPost(`/${ig}/media`, { media_type: "CAROUSEL", children: filhos.join(","), caption: legenda }, token);
        robo = { ...robo, containerId: c.id };
      } else {
        const m = midias[0];
        let body: Record<string, unknown>;
        if (post.tipo === "story") {
          body = m.tipo === "video" ? { media_type: "STORIES", video_url: await urlMidia(m) } : { media_type: "STORIES", image_url: await urlMidia(m) };
        } else if (m.tipo === "video" || post.tipo === "reels") {
          if (m.tipo !== "video") throw new Error("Reels precisa de um vídeo.");
          body = { media_type: "REELS", video_url: await urlMidia(m), caption: legenda, share_to_feed: true };
        } else {
          body = { image_url: await urlMidia(m), caption: legenda };
        }
        const c = await gPost(`/${ig}/media`, body, token);
        robo = { ...robo, containerId: c.id };
      }
      await salvarRobo({ containerId: robo.containerId, filhos: robo.filhos || [] });
    }

    if ((await esperar(robo.containerId)) === "AGUARDE") {
      await salvarRobo({ estado: "processando", containerId: robo.containerId });
      return { ok: true, pendente: true, msg: "O Instagram ainda está processando o vídeo. O robô termina a publicação em alguns minutos." };
    }

    const pub = await gPost(`/${ig}/media_publish`, { creation_id: robo.containerId }, token);
    let link = "";
    try { link = (await gGet(`/${pub.id}?fields=permalink`, token)).permalink || ""; } catch (_) { /* opcional */ }
    const agora = new Date().toISOString();
    await salvarRobo(
      { estado: "ok", erro: "", containerId: "", filhos: [], mediaId: pub.id, em: agora },
      { status: "publicado", publicadoEm: agora, publicadoPor: porQuem, link: link || post.link || "", avisado: true },
    );
    return { ok: true, publicado: true, link, mediaId: pub.id };
  } catch (e) {
    const msg = (e as Error)?.message || String(e);
    // limpa contêineres se o erro foi de mídia/expiração, para a próxima tentativa recriar
    await salvarRobo({ estado: "erro", erro: msg, em: new Date().toISOString(), erroAvisado: false, containerId: "", filhos: [] }).catch(() => {});
    return { ok: false, erro: msg };
  }
}

// ── Conexão (OAuth) ──────────────────────────────────────────────────
async function linkConexao(contaId: string, dias: number) {
  const exp = Date.now() + Math.max(1, Math.min(30, dias || 1)) * 86400000;
  const corpo = b64url(JSON.stringify({ c: contaId, e: exp }));
  const state = corpo + "." + (await assinar("state:" + corpo));
  const q = new URLSearchParams({
    client_id: IG_APP_ID,
    redirect_uri: REDIRECT_URI,
    response_type: "code",
    scope: SCOPES,
    state,
    force_reauth: "true",
  });
  return { url: "https://www.instagram.com/oauth/authorize?" + q.toString(), expira: new Date(exp).toISOString() };
}
function voltarParaApp(params: Record<string, string>) {
  const q = new URLSearchParams(params).toString();
  if (!APP_URL) return new Response("Resultado da conexão: " + q + "\n\nConfigure o segredo APP_URL para voltar ao sistema automaticamente.", { status: 200, headers: { "Content-Type": "text/plain; charset=utf-8" } });
  return new Response(null, { status: 302, headers: { Location: `${APP_URL}/?${q}` } });
}
async function callback(url: URL) {
  if (url.searchParams.get("error")) return voltarParaApp({ ig_conexao: "negado" });
  const code = (url.searchParams.get("code") || "").replace(/#_$/, "");
  const state = url.searchParams.get("state") || "";
  const [corpo, sig] = state.split(".");
  if (!corpo || !sig || !iguais(sig, await assinar("state:" + corpo))) return voltarParaApp({ ig_conexao: "erro", m: "Link de conexão inválido." });
  let st: { c: string; e: number };
  try { st = JSON.parse(deb64url(corpo)); } catch (_) { return voltarParaApp({ ig_conexao: "erro", m: "Link de conexão inválido." }); }
  if (Date.now() > st.e) return voltarParaApp({ ig_conexao: "erro", m: "Este link de conexão expirou. Peça um novo." });
  try {
    // 1) código → token curto
    const f = new URLSearchParams({ client_id: IG_APP_ID, client_secret: IG_APP_SECRET, grant_type: "authorization_code", redirect_uri: REDIRECT_URI, code });
    const r1 = await fetch("https://api.instagram.com/oauth/access_token", { method: "POST", body: f });
    const j1 = await r1.json().catch(() => ({}));
    const curto = j1?.data?.[0]?.access_token || j1?.access_token;
    if (!curto) throw new Error(j1?.error_message || j1?.error?.message || "Não foi possível obter o acesso.");
    // 2) token curto → token longo (60 dias)
    const r2 = await fetch(`https://graph.instagram.com/access_token?grant_type=ig_exchange_token&client_secret=${encodeURIComponent(IG_APP_SECRET)}&access_token=${encodeURIComponent(curto)}`);
    const j2 = await r2.json().catch(() => ({}));
    if (!j2.access_token) throw new Error(mensagemErro(j2, r2.status));
    const token = j2.access_token as string;
    const expira = new Date(Date.now() + (Number(j2.expires_in) || 5184000) * 1000).toISOString();
    // 3) quem é a conta
    const eu = await gGet(`/me?fields=user_id,username,account_type,profile_picture_url`, token);
    const igId = String(eu.user_id || eu.id || j1?.data?.[0]?.user_id || j1?.user_id || "");
    if (!igId) throw new Error("Não foi possível identificar a conta.");
    const { error } = await db.from("ig_tokens").upsert({
      conta_id: st.c, ig_user_id: igId, username: eu.username || "", access_token: token, expires_at: expira, updated_at: new Date().toISOString(),
    });
    if (error) throw new Error("Não foi possível guardar a conexão (tabela ig_tokens). " + error.message);
    let nomeConta = "";
    await gravar((d) => {
      const c = d.contas.find((x: Payload) => x.id === st.c);
      if (c) {
        nomeConta = c.nome || "";
        c.conexao = { username: eu.username || "", igUserId: igId, tipoConta: eu.account_type || "", foto: eu.profile_picture_url || "", conectadoEm: new Date().toISOString(), expiraEm: expira, erro: "" };
      }
      return d;
    });
    return voltarParaApp({ ig_conexao: "ok", u: eu.username || "", c: nomeConta });
  } catch (e) {
    return voltarParaApp({ ig_conexao: "erro", m: (e as Error).message || "Falha na conexão." });
  }
}

// ── Renovação de acesso (60 dias) ────────────────────────────────────
async function renovarTokens() {
  const { data } = await db.from("ig_tokens").select("*");
  const limite = Date.now() + 20 * 86400000;
  let renovados = 0;
  for (const t of data || []) {
    const exp = Date.parse(t.expires_at || "0");
    const idade = Date.now() - Date.parse(t.updated_at || "0");
    if (exp > limite || idade < 26 * 3600000) continue;
    try {
      const r = await fetch(`https://graph.instagram.com/refresh_access_token?grant_type=ig_refresh_token&access_token=${encodeURIComponent(t.access_token)}`);
      const j = await r.json().catch(() => ({}));
      if (!j.access_token) throw new Error(mensagemErro(j, r.status));
      const novaExp = new Date(Date.now() + (Number(j.expires_in) || 5184000) * 1000).toISOString();
      await db.from("ig_tokens").update({ access_token: j.access_token, expires_at: novaExp, updated_at: new Date().toISOString() }).eq("conta_id", t.conta_id);
      await gravar((d) => { const c = d.contas.find((x: Payload) => x.id === t.conta_id); if (c && c.conexao) { c.conexao.expiraEm = novaExp; c.conexao.erro = ""; } return d; });
      renovados++;
    } catch (e) {
      await gravar((d) => { const c = d.contas.find((x: Payload) => x.id === t.conta_id); if (c && c.conexao) c.conexao.erro = (e as Error).message; return d; }).catch(() => {});
    }
  }
  return renovados;
}

// ── Rodada do robô (cron) ────────────────────────────────────────────
async function rodadaCron() {
  const t0 = Date.now();
  const d = await lerPayload();
  const { data: toks } = await db.from("ig_tokens").select("conta_id");
  const conectadas = new Set((toks || []).map((t: Payload) => t.conta_id));
  const agora = Date.now();
  const fila = (d.posts || []).filter((p: Payload) => {
    if (p.status !== "agendado" || p.autoPublicar !== true || !conectadas.has(p.contaId)) return false;
    const r = p.robo || {};
    if (r.estado === "processando") return true;
    if (r.estado === "erro") return false; // não fica tentando sozinho: alguém precisa clicar em "tentar de novo"
    const q = quandoPost(p);
    // publica na hora (até 1 min antes) e só se não estiver atrasado demais (12h), para não soltar posts antigos de surpresa
    return q <= agora + 60000 && agora - q < 12 * 3600000;
  }).sort((a: Payload, b: Payload) => quandoPost(a) - quandoPost(b));
  const resultados: Payload[] = [];
  for (const p of fila) {
    const resta = 130000 - (Date.now() - t0);
    if (resta < 20000) break;
    const r = await publicarPost(p.id, "🤖 Robô (automático)", Math.min(90000, resta - 15000));
    resultados.push({ id: p.id, ...r });
  }
  let renovados = 0;
  if (Date.now() - t0 < 100000) renovados = await renovarTokens();
  return { fila: fila.length, resultados, renovados };
}

// ── Usuário logado no sistema ────────────────────────────────────────
async function usuarioDe(req: Request) {
  const auth = req.headers.get("authorization") || "";
  const jwt = auth.replace(/^Bearer\s+/i, "");
  if (!jwt) return null;
  const { data, error } = await db.auth.getUser(jwt);
  if (error || !data?.user) return null;
  const u = data.user;
  return { id: u.id, nome: (u.user_metadata?.name as string) || (u.email || "").split("@")[0] || "Usuário" };
}

async function api(req: Request) {
  const user = await usuarioDe(req);
  if (!user) return json({ ok: false, erro: "Faça login no sistema novamente." }, 401);
  const body = await req.json().catch(() => ({}));
  const acao = body.acao;

  if (acao === "estado") {
    let tabela = true, conectadas: Payload[] = [];
    const { data, error } = await db.from("ig_tokens").select("conta_id,username,expires_at,updated_at");
    if (error) tabela = false; else conectadas = data || [];
    return json({
      ok: true, versao: 1,
      metaConfigurado: !!(IG_APP_ID && IG_APP_SECRET), appUrl: APP_URL, assinaturaPropria: !!env("IG_SIGN_SECRET"),
      cronSecret: !!CRON_SECRET, tabela, redirectUri: REDIRECT_URI, conectadas,
    });
  }
  if (!IG_APP_ID || !IG_APP_SECRET) return json({ ok: false, erro: "Faltam os segredos IG_APP_ID e IG_APP_SECRET na função." });

  if (acao === "link-conexao") {
    if (!body.contaId) return json({ ok: false, erro: "Conta não informada." });
    return json({ ok: true, ...(await linkConexao(String(body.contaId), Number(body.dias) || 1)) });
  }
  if (acao === "publicar") {
    if (!body.postId) return json({ ok: false, erro: "Post não informado." });
    return json(await publicarPost(String(body.postId), user.nome));
  }
  if (acao === "tentar-de-novo") {
    await gravar((d) => { const x = d.posts.find((p: Payload) => p.id === body.postId); if (x && x.robo) x.robo = { ...x.robo, estado: "", erro: "" }; return d; });
    return json(await publicarPost(String(body.postId), user.nome));
  }
  if (acao === "desconectar") {
    await db.from("ig_tokens").delete().eq("conta_id", String(body.contaId));
    await gravar((d) => { const c = d.contas.find((x: Payload) => x.id === body.contaId); if (c) delete c.conexao; return d; });
    return json({ ok: true });
  }
  if (acao === "testar") {
    const tk = await tokenDaConta(String(body.contaId));
    if (!tk) return json({ ok: false, erro: "Conta não conectada." });
    try {
      const eu = await gGet(`/me?fields=user_id,username,account_type`, tk.access_token);
      let limite: Payload = null;
      try { limite = (await gGet(`/${tk.ig_user_id}/content_publishing_limit?fields=quota_usage,config`, tk.access_token))?.data?.[0] || null; } catch (_) { /* opcional */ }
      return json({ ok: true, username: eu.username, tipoConta: eu.account_type, usoHoje: limite?.quota_usage ?? null, limiteDia: limite?.config?.quota_total ?? null, expiraEm: tk.expires_at });
    } catch (e) { return json({ ok: false, erro: (e as Error).message }); }
  }
  return json({ ok: false, erro: "Ação desconhecida." }, 400);
}

// ── Roteador ─────────────────────────────────────────────────────────
Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: CORS });
  const url = new URL(req.url);
  const rota = url.pathname.replace(/^.*\/instagram/, "") || "/";
  try {
    if (req.method === "GET" && rota.startsWith("/callback")) return await callback(url);
    if (req.method === "GET" && rota.startsWith("/midia/")) return await servirMidia(url);
    if (req.method === "POST" && rota.startsWith("/cron")) {
      if (!CRON_SECRET || !iguais(req.headers.get("x-cron-secret") || "", CRON_SECRET)) return json({ ok: false, erro: "não autorizado" }, 401);
      return json({ ok: true, ...(await rodadaCron()) });
    }
    if (req.method === "POST" && (rota.startsWith("/api") || rota === "/")) return await api(req);
    return json({ ok: false, erro: "rota não encontrada" }, 404);
  } catch (e) {
    console.error(e);
    return json({ ok: false, erro: (e as Error)?.message || "erro interno" }, 500);
  }
});
