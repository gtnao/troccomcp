import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { z } from "zod";
import { zodToJsonSchema } from "zod-to-json-schema";

const VERSION = "0.1.0";

type RequestOptions = {
  method?: "GET" | "POST" | "PUT" | "PATCH" | "DELETE";
  body?: unknown;
};

async function troccoRequest<T>(
  url: string,
  options: RequestOptions = {},
): Promise<T> {
  const headers = {
    Accept: "application/json",
    "Content-Type": "application/json",
    "User-Agent": `troccomcp/${VERSION}`,
    Authorization: `Token ${process.env.TROCCO_API_KEY}`,
  };
  const response = await fetch(url, {
    method: options.method || "GET",
    headers,
    body: options.body ? JSON.stringify(options.body) : undefined,
  });
  if (!response.ok) {
    throw new Error(`TROCCO API request failed: ${response.status}`);
  }
  return await response.json();
}

const ListConnectionsInputSchema = z.object({
  connectionType: z.enum([
    "bigquery",
    "gcs",
    "google_spreadsheet",
    "snowflake",
    "mysql",
    "s3",
    "salesforce",
    "postgresql",
    "google_analytics4",
  ]),
});

type Connection = {
  id: string;
  name: string;
  description: string;
};

async function listConnections(
  input: z.infer<typeof ListConnectionsInputSchema>,
) {
  const url = `https://trocco.io/api/connections/${input.connectionType}`;
  return await withPagination<Connection>(url);
}

type PaginationResponse<Item> = {
  items: Item[];
  next_cursor: string | null;
};

async function withPagination<Item>(
  url: string,
  params: Record<string, string> = {},
  options: RequestOptions = {},
): Promise<Item[]> {
  const items = [];
  let nextCursor: string | null = null;
  do {
    const nextParams = new URLSearchParams({
      ...params,
      ...(nextCursor == null
        ? { limit: "5" }
        : { limit: "5", cursor: nextCursor }),
    });
    const nextUrl = `${url}?${nextParams}`;
    const data: PaginationResponse<Item> = await troccoRequest<
      PaginationResponse<Item>
    >(nextUrl, options);
    items.push(...data.items);
    nextCursor = data.next_cursor;
  } while (nextCursor != null);
  return items;
}

const server = new Server(
  {
    name: "troccomcp",
    version: VERSION,
  },
  {
    capabilities: {
      tools: {},
    },
  },
);

server.setRequestHandler(ListToolsRequestSchema, async () => {
  return {
    tools: [
      {
        name: "list_connections",
        description: "List TROCCO connections of a given type",
        inputSchema: zodToJsonSchema(ListConnectionsInputSchema),
      },
    ],
  };
});

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  if (!request.params.arguments) {
    throw new Error("Arguments are required");
  }
  switch (request.params.name) {
    case "list_connections": {
      const input = ListConnectionsInputSchema.parse(request.params.arguments);
      const connections = await listConnections(input);
      return {
        content: [{ type: "text", text: JSON.stringify(connections, null, 2) }],
      };
    }
    default: {
      throw new Error(`Unknown tool name: ${request.params.name}`);
    }
  }
});

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("TROCCO MCP Server running on stdio");
}

main().catch((error) => {
  console.error("Fatal error in main():", error);
  process.exit(1);
});
