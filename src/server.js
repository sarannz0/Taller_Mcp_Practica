import { z } from "zod";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";

import { pool } from "./db.js";
import { guardSql } from "./guard.js";
import { beginTx, commitTx, rollbackTx, getTxConn } from "./tx.js";
import { introspect } from "./introspect.js";
import { ListResourcesRequestSchema, ReadResourceRequestSchema } from "@modelcontextprotocol/sdk/types.js";

// ---------- Helpers DDL (estructurado) ----------
function qid(name) {
  // quoted identifier básico (no permite backticks dentro)
  if (!/^[a-zA-Z0-9_]+$/.test(name)) throw new Error(`Identificador inválido: ${name}`);
  return `\`${name}\``;
}

function buildCreateTableSql(input) {
  const { table, columns, primaryKey, unique, engine, charset } = input;

  const colDefs = columns.map(c => {
    const parts = [qid(c.name), c.type];
    parts.push(c.nullable ? "NULL" : "NOT NULL");
    if (c.autoIncrement) parts.push("AUTO_INCREMENT");
    if (c.default != null) parts.push(`DEFAULT ${c.default}`); // acepta CURRENT_TIMESTAMP, etc.
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

// ---------- Helpers DDL extendidos ----------
function buildDropTableSql(input) {
  const { table, ifExists } = input;
  return `DROP TABLE ${ifExists ? "IF EXISTS " : ""}${qid(table)};`;
}

function buildAlterTableColumnSql(input) {
  const { table, action, column, type, nullable, autoIncrement, default: def, afterColumn } = input;
  const parts = [qid(column), type];
  
  parts.push(nullable ? "NULL" : "NOT NULL");
  if (autoIncrement) parts.push("AUTO_INCREMENT");
  if (def != null) parts.push(`DEFAULT ${def}`);
  if (afterColumn) parts.push(`AFTER ${qid(afterColumn)}`);

  return `ALTER TABLE ${qid(table)} ${action} COLUMN ${parts.join(" ")};`;
}

// ---------- MCP Server ----------
const server = new McpServer({ name: "mcp-mysql", version: "1.0.0" });

// Tool: mysql_query (CRUD + consultas)
// Tool: mysql_query (CRUD + consultas)
server.tool(
  "mysql_query",
  "Ejecuta SQL parametrizado (SELECT/DML). DDL se bloquea si ALLOW_DDL=false.",
  {
    sql: z.string(),
    params: z.array(z.any()).default([]),
    mode: z.enum(["read", "write"]).default("read"),
    transactionId: z.string().optional()
  },
  async (args) => {
    const safeSql = guardSql(args.sql, args.mode);
    const executor = args.transactionId ? getTxConn(args.transactionId) : pool;

    const [rows, fields] = await executor.query(safeSql, args.params);
    return { content: [{ type: "text", text: JSON.stringify({ rows, fields }, null, 2) }] };
  }
);

// Tools: transacciones
// Tools: transacciones
server.tool(
  "mysql_transaction_begin",
  "Inicia transacción y devuelve transactionId",
  {},
  async () => {
    const transactionId = await beginTx();
    return { content: [{ type: "text", text: JSON.stringify({ transactionId }) }] };
  }
);

server.tool(
  "mysql_transaction_commit",
  "Confirma transacción",
  { transactionId: z.string() },
  async (args) => {
    await commitTx(args.transactionId);
    return { content: [{ type: "text", text: JSON.stringify({ ok: true }) }] };
  }
);

server.tool(
  "mysql_transaction_rollback",
  "Revierte transacción",
  { transactionId: z.string() },
  async (args) => {
    await rollbackTx(args.transactionId);
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
    return { content: [{ type: "text", text: JSON.stringify(meta, null, 2) }] };
  }
);

// Tool: create table (DDL estructurado)
server.tool(
  "mysql_create_table",
  "Crea tabla a partir de JSON (requiere ALLOW_DDL=true).",
  {
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
  },
  async (args) => {
    // La librería McpServer ya validó 'args' por ti con zod
    const sql = buildCreateTableSql(args);
    guardSql(sql, "write"); // aquí se bloqueará si ALLOW_DDL=false
    await pool.query(sql);

    return { content: [{ type: "text", text: JSON.stringify({ ok: true, sql }, null, 2) }] };
  }
);

// Tool: drop table
server.tool(
  "mysql_drop_table",
  "Elimina una tabla de forma estructurada (requiere ALLOW_DDL=true).",
  {
    table: z.string(),
    ifExists: z.boolean().optional().default(true)
  },
  async (args) => {
    const sql = buildDropTableSql(args);
    guardSql(sql, "write");
    await pool.query(sql);
    return { content: [{ type: "text", text: JSON.stringify({ ok: true, sql }, null, 2) }] };
  }
);

// Tool: alter table column
server.tool(
  "mysql_alter_table_column",
  "Añade o modifica una columna de una tabla (requiere ALLOW_DDL=true).",
  {
    table: z.string(),
    action: z.enum(["ADD", "MODIFY"]),
    column: z.string(),
    type: z.string(),
    nullable: z.boolean(),
    autoIncrement: z.boolean().optional(),
    default: z.string().optional(),
    afterColumn: z.string().optional()
  },
  async (args) => {
    const sql = buildAlterTableColumnSql(args);
    guardSql(sql, "write");
    await pool.query(sql);
    return { content: [{ type: "text", text: JSON.stringify({ ok: true, sql }, null, 2) }] };
  }
);

// Tool: add FK
// Tool: add FK
server.tool(
  "mysql_add_foreign_key",
  "Agrega FK (requiere ALLOW_DDL=true).",
  {
    table: z.string(),
    name: z.string(),
    column: z.string(),
    refTable: z.string(),
    refColumn: z.string(),
    onDelete: z.enum(["RESTRICT", "CASCADE", "SET NULL", "NO ACTION"]).optional(),
    onUpdate: z.enum(["RESTRICT", "CASCADE", "SET NULL", "NO ACTION"]).optional()
  },
  async (args) => {
    // La librería ya valida 'args' automáticamente
    const sql = buildAddForeignKeySql(args);
    guardSql(sql, "write");
    await pool.query(sql);
    return { content: [{ type: "text", text: JSON.stringify({ ok: true, sql }, null, 2) }] };
  }
);

// Tool: create index
server.tool(
  "mysql_create_index",
  "Crea índice (requiere ALLOW_DDL=true).",
  {
    table: z.string(),
    name: z.string(),
    columns: z.array(z.string()).min(1),
    unique: z.boolean().optional()
  },
  async (args) => {
    const sql = buildCreateIndexSql(args);
    guardSql(sql, "write");
    await pool.query(sql);
    return { content: [{ type: "text", text: JSON.stringify({ ok: true, sql }, null, 2) }] };
  }
);

server.resource(
  "database_schema",
  "mysql://schema",
  async (uri) => {
    // Consultamos el esquema directamente a MySQL
    const [rows] = await pool.query(`
      SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY
      FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE()
    `);

    // Agrupamos la información por tablas
    const schemaData = rows.reduce((acc, row) => {
      if (!acc[row.TABLE_NAME]) {
        acc[row.TABLE_NAME] = [];
      }
      acc[row.TABLE_NAME].push({
        columna: row.COLUMN_NAME,
        tipo: row.DATA_TYPE,
        nulo: row.IS_NULLABLE,
        llave: row.COLUMN_KEY
      });
      return acc;
    }, {});

    return {
      contents: [
        {
          uri: uri.href,
          mimeType: "application/json",
          text: JSON.stringify(schemaData, null, 2)
        }
      ]
    };
  }
);

server.prompt(
  "db_task",
  "Genera instrucciones para tareas de base de datos (DDL).",
  {
    goal: z.string().describe("El objetivo de la base de datos (ej: 'crear tabla invoices')")
  },
  (args) => {
    return {
      messages: [
        { 
          role: "user",
          content: {
            type: "text",
            text: `Actúa como un DBA experto en MySQL. Mi objetivo es: "${args.goal}". 
            
            Por favor, sigue estos pasos usando mis herramientas disponibles:
            1. Analiza si la tabla requiere llaves foráneas basándote en el recurso 'mysql://schema'.
            2. Usa la herramienta 'mysql_create_table' con un JSON bien estructurado.
            3. Si el objetivo menciona velocidad o búsqueda, usa 'mysql_create_index' después de crear la tabla.
            4. Asegúrate de que todos los nombres de tablas y columnas sigan la convención snake_case.`
          }
        }
      ]
    };
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