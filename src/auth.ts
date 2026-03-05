import { bearer } from "@elysiajs/bearer";
import { Elysia } from "elysia";
import users from "../users.json";

const tokens = new Set(Object.values(users));

export const auth = new Elysia({ name: "auth" })
  .use(bearer())
  .onBeforeHandle({ as: "global" }, ({ bearer, status }) => {
    if (!bearer || !tokens.has(bearer)) {
      return status(401, "Unauthorized");
    }
  });
