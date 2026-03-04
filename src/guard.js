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


    // 1. Verificamos si la consulta es un DELETE o un UPDATE usando la variable 's'
    const isDelete = /^DELETE\s/i.test(s);
    const isUpdate = /^UPDATE\s/i.test(s);

    // 2. Verificamos si la consulta contiene la palabra WHERE
    const hasWhere = /\bWHERE\b/i.test(s);

    // 3. Aplicamos el bloqueo
    if (isDelete && !hasWhere) {
        throw new Error("🛡️ Modo seguro activado: Operación rechazada. No se permite ejecutar DELETE sin una cláusula WHERE.");
    }

    if (isUpdate && !hasWhere) {
        throw new Error("🛡️ Modo seguro activado: Operación rechazada. No se permite ejecutar UPDATE sin una cláusula WHERE.");
    }

    // retornamos el string
    return s;
}