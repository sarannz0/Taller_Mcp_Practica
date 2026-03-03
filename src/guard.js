const ALLOW_DDL = process.env.ALLOW_DDL === "true";
const DDL_REGEX = /\b(CREATE|ALTER|DROP|TRUNCATE|RENAME)\b/i;
const DANGEROUS = /\b(INTO\s+OUTFILE|LOAD_FILE|LOAD\s+DATA)\b/i;

export function guardSql(sql, mode = "read") {
  const s = String(sql || "").trim();
  if (!s) throw new Error("SQL vacío");

  if (DANGEROUS.test(s)) throw new Error("SQL bloqueado (OUTFILE/LOAD_FILE/LOAD DATA).");

  if (mode === "read" && !/^SELECT\b/i.test(s)) {
    throw new Error("Modo read solo permite SELECT.");
  }

  if (!ALLOW_DDL && DDL_REGEX.test(s)) {
    throw new Error("DDL deshabilitado. Usa ALLOW_DDL=true si lo necesitas.");
  }

  return s;
}