async function main() {
  const chainId = "1";
  const topic =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
  const token = "replace-with-secure-token";
  const url = `http://localhost:3000/${chainId}/logs/stream?token=${token}&topic=${topic}&fromBlock=0&toBlock=5000000`;

  console.log(`Starting SSE stream from ${url}...`);

  try {
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`Unexpected response: ${response.statusText}`);
    }

    const reader = response.body?.getReader();
    if (!reader) {
      throw new Error("No reader available");
    }

    let totalLogs = 0;
    let chunks = 0;
    let lastBlockNumber = 0;

    const startTime = Date.now();
    let lastLogCount = 0;
    let lastLogTime = startTime;

    const decoder = new TextDecoder();
    let buffer = "";

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop() || "";

      for (const line of lines) {
        if (line.startsWith("data: ")) {
          chunks++;
          const data = JSON.parse(line.slice(6));
          if (Array.isArray(data)) {
            totalLogs += data.length;
            if (data.length > 0) {
              const lastLog = data[data.length - 1];
              lastBlockNumber =
                typeof lastLog.blockNumber === "number"
                  ? lastLog.blockNumber
                  : lastLog.block_number;
            }
          }

          const now = Date.now();
          if (totalLogs - lastLogCount >= 1000 || now - lastLogTime >= 1000) {
            const elapsedSec = (now - lastLogTime) / 1000;
            const logsSinceLast = totalLogs - lastLogCount;
            const rate =
              elapsedSec > 0 ? Math.round(logsSinceLast / elapsedSec) : 0;
            const memoryMB = Math.round(
              process.memoryUsage().heapUsed / 1024 / 1024,
            );
            console.log(
              `Progress: ${totalLogs.toLocaleString()} logs received (${rate.toLocaleString()} logs/sec) | Last Block: ${lastBlockNumber} | Memory: ${memoryMB}MB...`,
            );
            lastLogCount = totalLogs;
            lastLogTime = now;
          }
        }
      }
    }

    const elapsedMs = Date.now() - startTime;
    console.log(
      `SSE Stream test successful! Received ${totalLogs.toLocaleString()} logs across ${chunks} chunks in ${elapsedMs}ms.`,
    );
    process.exit(0);
  } catch (err) {
    console.error("SSE Stream failed:", err);
    process.exit(1);
  }
}

main();
