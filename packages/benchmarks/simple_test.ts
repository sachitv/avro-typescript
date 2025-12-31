// Simple test bench
Deno.bench("simple test", () => {
  const x = 1 + 1;
  if (x !== 2) throw new Error("Math failed");
});