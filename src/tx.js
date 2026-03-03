import { pool } from "./db.js";
import crypto from "crypto";

const txMap = new Map(); // id -> conn

export async function beginTx() {
  const conn = await pool.getConnection();
  await conn.beginTransaction();
  const id = crypto.randomUUID();
  txMap.set(id, conn);
  return id;
}

export function getTxConn(id) {
  const conn = txMap.get(id);
  if (!conn) throw new Error("transactionId inválido o expirado.");
  return conn;
}

export async function commitTx(id) {
  const conn = getTxConn(id);
  await conn.commit();
  conn.release();
  txMap.delete(id);
}

export async function rollbackTx(id) {
  const conn = getTxConn(id);
  await conn.rollback();
  conn.release();
  txMap.delete(id);
}