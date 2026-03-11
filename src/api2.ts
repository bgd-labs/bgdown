import { createServer } from "node:http2";
import { create } from "@bufbuild/protobuf";
import type { ConnectRouter } from "@connectrpc/connect";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import {
  type Log,
  LogSchema,
  LogService,
  type StreamLogsRequest,
} from "./api2/gen/logs_pb.ts";
import { clickhouse } from "./clickhouse.ts";
import env from "./env.ts";
import { buildLogsQuery } from "./routes/logs.ts";

const PORT = env.PORT + 1;
const MAX_THREADS = 2;

export const routes = (router: ConnectRouter) =>
  router.service(LogService, {
    async *streamLogs(req: StreamLogsRequest) {
      console.log(
        `StreamLogs request (H2) for topic: ${req.topic} on chain: ${req.chainId}`,
      );

      const opts: Parameters<typeof buildLogsQuery>[0] = {
        chainId: req.chainId,
        topic: req.topic,
      };
      if (req.address) opts.address = req.address;
      if (req.fromBlock !== undefined) opts.fromBlock = Number(req.fromBlock);
      if (req.toBlock !== undefined) opts.toBlock = Number(req.toBlock);

      const { query: sql, query_params } = buildLogsQuery(opts);

      const result = await clickhouse.query({
        query: sql,
        query_params,
        format: "JSONCompactEachRow",
        clickhouse_settings: {
          max_threads: MAX_THREADS,
        },
      });

      let lastMemoryLogTime = Date.now();
      let totalLogs = 0;

      for await (const rows of result.stream()) {
        const now = Date.now();
        if (now - lastMemoryLogTime >= 10_000) {
          const memoryMB = Math.round(
            process.memoryUsage().heapUsed / 1024 / 1024,
          );
          console.log(
            `[StreamLogs] Processed ${totalLogs.toLocaleString()} logs | Memory usage: ${memoryMB}MB`,
          );
          lastMemoryLogTime = now;
        }

        const logs: Log[] = [];

        for (const row of rows) {
          const r = row.json<string[]>();
          const topics: string[] = [];
          if (r[8]) topics.push(r[8]);
          if (r[9]) topics.push(r[9]);
          if (r[10]) topics.push(r[10]);
          if (r[11]) topics.push(r[11]);

          logs.push(
            create(LogSchema, {
              address: r[6],
              blockHash: r[2],
              blockNumber: BigInt(r[0]),
              timestamp: BigInt(r[1]),
              data: r[7],
              logIndex: Number(r[5]),
              topics,
              transactionHash: r[3],
              transactionIndex: Number(r[4]),
            }),
          );
        }

        totalLogs += logs.length;
        yield { logs };
      }
    },
  });

const server = createServer(connectNodeAdapter({ routes }));

server.listen(PORT, () => {
  console.log(`ConnectRPC API2 (HTTP/2) listening on port ${PORT}`);
});
