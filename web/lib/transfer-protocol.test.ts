import { describe, expect, test } from "bun:test";
import {
  CODE_LENGTH,
  cleanRoomCode,
  fmtSize,
  isValidRoomCode,
  numberedFileName,
  safeRelativeName,
} from "./transfer-protocol";

describe("room codes", () => {
  test("normalizes pasted codes and excludes ambiguous characters", () => {
    expect(cleanRoomCode(" abcd-2345 ")).toBe("ABCD2345");
    expect(cleanRoomCode("O1ILabcd2345")).toBe("LABCD234");
  });

  test("requires the complete supported alphabet", () => {
    expect(isValidRoomCode("ABCD2345")).toBe(true);
    expect(isValidRoomCode("ABCD1234")).toBe(false);
    expect(isValidRoomCode("ABCD234")).toBe(false);
    expect(CODE_LENGTH).toBe(8);
  });
});

describe("file metadata", () => {
  test("accepts nested relative names", () => {
    expect(safeRelativeName("photos/2026/image.jpg")).toBe(
      "photos/2026/image.jpg",
    );
    expect(safeRelativeName("photos\\image.jpg")).toBe("photos/image.jpg");
  });

  test("rejects traversal and malformed components", () => {
    expect(safeRelativeName("../secret")).toBeNull();
    expect(safeRelativeName("/absolute")).toBeNull();
    expect(safeRelativeName("folder//file")).toBeNull();
    expect(safeRelativeName(`folder/${"a".repeat(256)}`)).toBeNull();
  });

  test("creates collision-safe display names", () => {
    expect(numberedFileName("photo.jpg", 0)).toBe("photo.jpg");
    expect(numberedFileName("photo.jpg", 2)).toBe("photo (2).jpg");
    expect(numberedFileName(".env", 1)).toBe(".env (1)");
  });

  test("formats binary sizes", () => {
    expect(fmtSize(0)).toBe("0 B");
    expect(fmtSize(1536)).toBe("1.5 KB");
  });
});
