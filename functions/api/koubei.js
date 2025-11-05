export async function onRequestGet(context) {
  const url = new URL(context.request.url);
  const id = url.searchParams.get("id") || "197";
  const fileUrl = `https://china-auto-dashboard.pages.dev/api/koubei_summary_${id}.json`;

  const res = await fetch(fileUrl);
  if (!res.ok) {
    return new Response(JSON.stringify({ error: "File not found" }), {
      status: 404,
      headers: { "Content-Type": "application/json" },
    });
  }

  const data = await res.json();
  return new Response(JSON.stringify(data, null, 2), {
    headers: { "Content-Type": "application/json" },
  });
}
