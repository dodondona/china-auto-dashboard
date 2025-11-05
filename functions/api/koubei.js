// functions/api/koubei.js
export async function onRequestGet(context) {
  const url = new URL(context.request.url);
  const id = url.searchParams.get("id");

  if (!id) {
    return new Response(JSON.stringify({ error: "missing id" }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  }

  // Cloudflare PagesでホストされているJSONファイルを参照
  const fileUrl = `https://china-auto-dashboard.pages.dev/api/koubei_summary_${id}.json`;
  const res = await fetch(fileUrl);
  if (!res.ok) {
    return new Response(JSON.stringify({ error: "file not found" }), {
      status: 404,
      headers: { "Content-Type": "application/json" },
    });
  }

  const data = await res.json();
  return new Response(JSON.stringify(data), {
    headers: { "Content-Type": "application/json" },
  });
}
