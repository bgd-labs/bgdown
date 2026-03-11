import { createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-node";
import { LogService } from "./api2/gen/logs_pb.ts";

const transport = createConnectTransport({
  baseUrl: "http://localhost:3001",
  httpVersion: "2",
});

const client = createClient(LogService, transport);

async function main() {
  console.log("Starting stream...");
  try {
    const stream = client.streamLogs({
      chainId: "1",
      topic:
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
      fromBlock: 0n,
      toBlock: 5000000n,
    });

    let totalLogs = 0;
    let chunks = 0;
    let lastBlockNumber = 0n;

    const startTime = Date.now();
    let lastLogCount = 0;
    let lastLogTime = startTime;

    for await (const res of stream) {
      chunks++;
      totalLogs += res.logs.length;
      if (res.logs.length > 0) {
        lastBlockNumber = res.logs[res.logs.length - 1].blockNumber;
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
    const elapsedMs = Date.now() - startTime;
    console.log(
      `Stream test successful! Received ${totalLogs.toLocaleString()} logs across ${chunks} chunks in ${elapsedMs}ms.`,
    );
    process.exit(0);
  } catch (err) {
    console.error("Stream failed:", err);
    process.exit(1);
  }
}

main();
