import { Elysia, t } from "elysia";
import users from "../users.json";

const tokens = new Set(Object.values(users));

export const auth = new Elysia({ name: "auth" })
  .guard({ query: t.Object({ token: t.String() }) })
  .onBeforeHandle({ as: "global" }, ({ query, status }) => {
    if (!query.token || !tokens.has(query.token)) {
      return status(401, "Unauthorized");
    }
  });
