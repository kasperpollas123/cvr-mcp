export default function Home() {
  return (
    <main style={{ fontFamily: "sans-serif", padding: "2rem", maxWidth: 600 }}>
      <h1>CVR MCP Server</h1>
      <p>MCP endpoint: <code>/api/mcp</code></p>
      <p>Tools: find_companies, count_companies, get_company, find_leads</p>
    </main>
  );
}
