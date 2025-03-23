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

// TODO: minimal schema for now
const CreateDatamartDefinitionInputSchema = z.object({
  name: z.string().min(1),
  dataWarehouseType: z.enum(["bigquery"]),
  description: z.string().optional(),
  isRunnableConcurrently: z.boolean(),
  datamartBigqueryOption: z
    .object({
      bigqueryConnectionId: z.number(),
      queryMode: z.enum(["insert"]),
      query: z.string(),
      destinationDataset: z.string(),
      destinationTable: z.string(),
      writeDisposition: z.enum(["append", "truncate"]),
    })
    .optional()
    .describe("required if dataWarehouseType is bigquery"),
});

type Connection = {
  id: string;
  name: string;
  description: string;
};

async function listConnections(
  input: z.infer<typeof ListConnectionsInputSchema>,
): Promise<Connection[]> {
  const url = `https://trocco.io/api/connections/${input.connectionType}`;
  const connections = await withPagination<Connection>(url);
  return connections.map((connection) => ({
    id: connection.id,
    name: connection.name,
    description: connection.description,
  }));
}

type DatamartDefinition = {
  id: number;
  name: string;
  data_warehouse_type: string;
  description: string;
  is_runnable_concurrently: boolean;
  datamart_bigquery_option?: {
    bigquery_connection_id: number;
    query_mode: string;
    query: string;
    destination_dataset: string;
    destination_table: string;
    write_disposition: string;
  };
};

async function createDatamartDefinition(
  input: z.infer<typeof CreateDatamartDefinitionInputSchema>,
): Promise<DatamartDefinition> {
  const url = `https://trocco.io/api/datamart_definitions`;
  const options: RequestOptions = {
    method: "POST",
    body: {
      name: input.name,
      data_warehouse_type: input.dataWarehouseType,
      description: input.description,
      is_runnable_concurrently: input.isRunnableConcurrently,
      datamart_bigquery_option: {
        bigquery_connection_id:
          input.datamartBigqueryOption?.bigqueryConnectionId,
        query_mode: input.datamartBigqueryOption?.queryMode,
        query: input.datamartBigqueryOption?.query,
        destination_dataset: input.datamartBigqueryOption?.destinationDataset,
        destination_table: input.datamartBigqueryOption?.destinationTable,
        write_disposition: input.datamartBigqueryOption?.writeDisposition,
      },
    },
  };
  const datamartDefinition = await troccoRequest<DatamartDefinition>(
    url,
    options,
  );
  return {
    id: datamartDefinition.id,
    name: datamartDefinition.name,
    data_warehouse_type: datamartDefinition.data_warehouse_type,
    description: datamartDefinition.description,
    is_runnable_concurrently: datamartDefinition.is_runnable_concurrently,
    datamart_bigquery_option: datamartDefinition.datamart_bigquery_option,
  };
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
      {
        name: "create_datamart_definition",
        description: "Create a TROCCO datamart definition",
        inputSchema: zodToJsonSchema(CreateDatamartDefinitionInputSchema),
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
    case "create_datamart_definition": {
      const input = CreateDatamartDefinitionInputSchema.parse(
        request.params.arguments,
      );
      const result = await createDatamartDefinition(input);
      return {
        content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
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
