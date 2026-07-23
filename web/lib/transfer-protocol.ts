export const CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
export const CODE_LENGTH = 8;
export const MAX_FILE_SIZE = 64 * 1024 * 1024 * 1024;
export const MAX_BUFFERED_FILE_SIZE = 128 * 1024 * 1024;

const MAX_RELATIVE_NAME_LENGTH = 4096;
const MAX_PATH_COMPONENTS = 64;
const MAX_PATH_COMPONENT_LENGTH = 255;

export function genCode(cryptoSource: Pick<Crypto, "getRandomValues"> = crypto) {
  const buf = new Uint32Array(CODE_LENGTH);
  cryptoSource.getRandomValues(buf);
  return Array.from(buf, (value) => CODE_CHARS[value % CODE_CHARS.length]).join("");
}

export function cleanRoomCode(value: string) {
  return Array.from(value.toUpperCase())
    .filter((character) => CODE_CHARS.includes(character))
    .slice(0, CODE_LENGTH)
    .join("");
}

export function isValidRoomCode(value: string) {
  return (
    value.length === CODE_LENGTH &&
    Array.from(value).every((character) => CODE_CHARS.includes(character))
  );
}

export function safeRelativeName(value: unknown): string | null {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > MAX_RELATIVE_NAME_LENGTH
  ) {
    return null;
  }
  const normalized = value.replaceAll("\\", "/");
  const parts = normalized.split("/");
  if (
    parts.length > MAX_PATH_COMPONENTS ||
    parts.some(
      (part) =>
        part.length === 0 ||
        part.length > MAX_PATH_COMPONENT_LENGTH ||
        part === "." ||
        part === ".." ||
        part.includes("\0"),
    )
  ) {
    return null;
  }
  return parts.join("/");
}

export function numberedFileName(name: string, index: number) {
  if (index === 0) return name;
  const dot = name.lastIndexOf(".");
  if (dot <= 0) return `${name} (${index})`;
  return `${name.slice(0, dot)} (${index})${name.slice(dot)}`;
}

export function fmtSize(bytes: number) {
  if (bytes >= 1024 ** 3) return `${(bytes / 1024 ** 3).toFixed(1)} GB`;
  if (bytes >= 1024 ** 2) return `${(bytes / 1024 ** 2).toFixed(1)} MB`;
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${bytes} B`;
}
