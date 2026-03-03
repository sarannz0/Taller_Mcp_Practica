import { z } from "zod";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";

import { pool } from "./db.js";
import { guardSql } from "./guard.js";
import { beginTx, commitTx, rollbackTx, getTxConn } from "./tx.js";
import { introspect } from "./introspect.js";

// ---------- Helpers DDL (estructurado) ----------
function qid(name) {
  if (!/^[a-zA-Z0-9_]+$/.test(name)) throw new Error(`Identificador inválido: ${name}`);
  return `\`${name}\``;
}

function buildCreateTableSql(input) {
  const { table, columns, primaryKey, unique, engine, charset } = input;

  const colDefs = columns.map(c => {
    const parts = [qid(c.name), c.type];
    parts.push(c.nullable ? "NULL" : "NOT NULL");
    if (c.autoIncrement) parts.push("AUTO_INCREMENT");
    if (c.default != null) parts.push(`DEFAULT ${c.default}`); 
    return parts.join(" ");
  });

  const constraints = [];
  if (primaryKey?.length) constraints.push(`PRIMARY KEY (${primaryKey.map(qid).join(", ")})`);
  if (unique?.length) {
    for (const u of unique) {
      constraints.push(`UNIQUE KEY ${qid(u.name)} (${u.columns.map(qid).join(", ")})`);
    }
  }

  const sql = [
    `CREATE TABLE ${qid(table)} (`,
    `  ${[...colDefs, ...constraints].join(",\n  ")}`,
    `) ENGINE=${engine || "InnoDB"} DEFAULT CHARSET=${charset || "utf8mb4"};`
  ].join("\n");

  return sql;
}

function buildAddForeignKeySql(input) {
  const { table, name, column, refTable, refColumn, onDelete, onUpdate } = input;
  return [
    `ALTER TABLE ${qid(table)}`,
    `ADD CONSTRAINT ${qid(name)}`,
    `FOREIGN KEY (${qid(column)}) REFERENCES ${qid(refTable)} (${qid(refColumn)})`,
    onDelete ? `ON DELETE ${onDelete}` : "",
    onUpdate ? `ON UPDATE ${onUpdate}` : "",
    `;`
  ].filter(Boolean).join(" ");
}

function buildCreateIndexSql(input) {
  const { table, name, columns, unique } = input;
  return `${unique ? "CREATE UNIQUE INDEX" : "CREATE INDEX"} ${qid(name)} ON ${qid(table)} (${columns.map(qid).join(", ")});`;
}

// ---------- MCP Server ----------
const server = new McpServer({ name: "mcp-mysql", version: "1.0.0" });

// Tool: mysql_query (CRUD + consultas)
server.tool(
  "mysql_query",
  {
    description: "Ejecuta SQL parametrizado (SELECT/DML). DDL se bloquea si ALLOW_DDL=false.",
    inputSchema: {
      type: "object",
      properties: {
        sql: { type: "string" },
        params: { type: "array", items: {} },
        mode: { type: "string", enum: ["read", "write"], default: "read" },
        transactionId: { type: "string" }
      },
      required: ["sql"]
    }
  },
  async (args) => {
    const parsed = z.object({
      sql: z.string(),
      params: z.array(z.any()).default([]),
      mode: z.enum(["read", "write"]).default("read"),
      transactionId: z.string().optional()
    }).parse(args);

    const safeSql = guardSql(parsed.sql, parsed.mode);
    const executor = parsed.transactionId ? getTxConn(parsed.transactionId) : pool;

    const [rows, fields] = await executor.query(safeSql, parsed.params);
    // CORRECCIÓN: type "text" y JSON.stringify
    return { content: [{ type: "text", text: JSON.stringify({ rows, fields }, null, 2) }] };
  }
);

// Tools: transacciones
server.tool(
  "mysql_transaction_begin",
  { description: "Inicia transacción y devuelve transactionId", inputSchema: { type: "object", properties: {} } },
  async () => {
    const transactionId = await beginTx();
    // CORRECCIÓN: type "text" y JSON.stringify
    return { content: [{ type: "text", text: JSON.stringify({ transactionId }) }] };
  }
);

server.tool(
  "mysql_transaction_commit",
  {
    description: "Confirma transacción",
    inputSchema: { type: "object", properties: { transactionId: { type: "string" } }, required: ["transactionId"] }
  },
  async (args) => {
    const { transactionId } = z.object({ transactionId: z.string() }).parse(args);
    await commitTx(transactionId);
    // CORRECCIÓN: type "text" y JSON.stringify
    return { content: [{ type: "text", text: JSON.stringify({ ok: true }) }] };
  }
);

server.tool(
  "mysql_transaction_rollback",
  {
    description: "Revierte transacción",
    inputSchema: { type: "object", properties: { transactionId: { type: "string" } }, required: ["transactionId"] }
  },
  async (args) => {
    const { transactionId } = z.object({ transactionId: z.string() }).parse(args);
    await rollbackTx(transactionId);
    // CORRECCIÓN: type "text" y JSON.stringify
    return { content: [{ type: "text", text: JSON.stringify({ ok: true }) }] };
  }
);

// Tool: introspect
server.tool(
  "mysql_introspect",
  {
    description: "Devuelve metadata (tablas, columnas, FKs, índices) desde INFORMATION_SCHEMA",
    inputSchema: { type: "object", properties: { schema: { type: "string" } } }
  },
  async (args) => {
    const { schema } = z.object({ schema: z.string().optional() }).parse(args);
    const meta = await introspect(schema);
    // CORRECCIÓN: type "text" y JSON.stringify
    return { content: [{ type: "text", text: JSON.stringify(meta, null, 2) }] };
  }
);

// Tool: create table (DDL estructurado)
server.tool(
  "mysql_create_table",
  {
    description: "Crea tabla a partir de JSON (requiere ALLOW_DDL=true).",
    inputSchema: {
      type: "object",
      properties: {
        table: { type: "string" },
        columns: {
          type: "array",
          items: {
            type: "object",
            properties: {
              name: { type: "string" },
              type: { type: "string" },
              nullable: { type: "boolean" },
              autoIncrement: { type: "boolean" },
              default: { type: "string" }
            },
            required: ["name", "type", "nullable"]
          }
        },
        primaryKey: { type: "array", items: { type: "string" } },
        unique: {
          type: "array",
          items: {
            type: "object",
            properties: {
              name: { type: "string" },
              columns: { type: "array", items: { type: "string" } }
            },
            required: ["name", "columns"]
          }
        },
        engine: { type: "string" },
        charset: { type: "string" }
      },
      required: ["table", "columns"]
    }
  },
  async (args) => {
    const input = z.object({
      table: z.string(),
      columns: z.array(z.object({
        name: z.string(),
        type: z.string(),
        nullable: z.boolean(),
        autoIncrement: z.boolean().optional(),
        default: z.string().optional()
      })).min(1),
      primaryKey: z.array(z.string()).optional(),
      unique: z.array(z.object({ name: z.string(), columns: z.array(z.string()).min(1) })).optional(),
      engine: z.string().optional(),
      charset: z.string().optional()
    }).parse(args);

    const sql = buildCreateTableSql(input);
    guardSql(sql, "write"); 
    await pool.query(sql);

    // CORRECCIÓN: type "text" y JSON.stringify
    return { content: [{ type: "text", text: JSON.stringify({ ok: true, sql }) }] };
  }
);

// Tool: add FK
server.tool(
  "mysql_add_foreign_key",
  {
    description: "Agrega FK (requiere ALLOW_DDL=true).",
    inputSchema: {
      type: "object",
      properties: {
        table: { type: "string" },
        name: { type: "string" },
        column: { type: "string" },
        refTable: { type: "string" },
        refColumn: { type: "string" },
        onDelete: { type: "string", enum: ["RESTRICT", "CASCADE", "SET NULL", "NO ACTION"] },
        onUpdate: { type: "string", enum: ["RESTRICT", "CASCADE", "SET NULL", "NO ACTION"] }
      },
      required: ["table", "name", "column", "refTable", "refColumn"]
    }
  },
  async (args) => {
    const input = z.object({
      table: z.string(),
      name: z.string(),
      column: z.string(),
      refTable: z.string(),
      refColumn: z.string(),
      onDelete: z.enum(["RESTRICT", "CASCADE", "SET NULL", "NO ACTION"]).optional(),
      onUpdate: z.enum(["RESTRICT", "CASCADE", "SET NULL", "NO ACTION"]).optional()
    }).parse(args);

    const sql = buildAddForeignKeySql(input);
    guardSql(sql, "write");
    await pool.query(sql);
    // CORRECCIÓN: type "text" y JSON.stringify
    return { content: [{ type: "text", text: JSON.stringify({ ok: true, sql }) }] };
  }
);

// Tool: create index
server.tool(
  "mysql_create_index",
  {
    description: "Crea índice (requiere ALLOW_DDL=true).",
    inputSchema: {
      type: "object",
      properties: {
        table: { type: "string" },
        name: { type: "string" },
        columns: { type: "array", items: { type: "string" } },
        unique: { type: "boolean" }
      },
      required: ["table", "name", "columns"]
    }
  },
  async (args) => {
    const input = z.object({
      table: z.string(),
      name: z.string(),
      columns: z.array(z.string()).min(1),
      unique: z.boolean().optional()
    }).parse(args);
 
    const sql = buildCreateIndexSql(input);
    guardSql(sql, "write");
    await pool.query(sql);
    // CORRECCIÓN: type "text" y JSON.stringify
    return { content: [{ type: "text", text: JSON.stringify({ ok: true, sql }) }] };
  }
);

// Arranque stdio
async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
}
main().catch((e) => {
  console.error(e);
  process.exit(1);
});