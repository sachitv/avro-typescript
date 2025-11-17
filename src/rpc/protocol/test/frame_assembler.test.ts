import { FrameAssembler } from "../frame_assembler.ts";
import { assertEquals } from "@std/assert";
import { describe, it } from "@std/testing/bdd";

describe("FrameAssembler", () => {
  it("push empty chunk", () => {
    const assembler = new FrameAssembler();
    const result = assembler.push(new Uint8Array(0));
    assertEquals(result, []);
  });

  it("push single frame", () => {
    const assembler = new FrameAssembler();
    const frame = new Uint8Array([1, 2, 3, 4]);
    // Create big-endian 32-bit length prefix
    const length = new Uint8Array(4);
    const view = new DataView(length.buffer);
    view.setUint32(0, frame.length, false); // big-endian
    const zero = new Uint8Array(4); // length 0 terminator
    const chunk = new Uint8Array(length.length + frame.length + zero.length);
    chunk.set(length, 0);
    chunk.set(frame, 4);
    chunk.set(zero, 4 + frame.length);
    const result = assembler.push(chunk);
    assertEquals(result, [frame]);
  });

  it("push multiple frames in one chunk", () => {
    const assembler = new FrameAssembler();
    const frame1 = new Uint8Array([1, 2]);
    const frame2 = new Uint8Array([3, 4, 5]);
    // Big-endian length for frame1
    const length1 = new Uint8Array(4);
    new DataView(length1.buffer).setUint32(0, frame1.length, false);
    // Big-endian length for frame2
    const length2 = new Uint8Array(4);
    new DataView(length2.buffer).setUint32(0, frame2.length, false);
    const zero = new Uint8Array(4); // terminator
    const chunk = new Uint8Array(
      length1.length + frame1.length + length2.length + frame2.length +
        zero.length,
    );
    let offset = 0;
    chunk.set(length1, offset);
    offset += 4;
    chunk.set(frame1, offset);
    offset += frame1.length;
    chunk.set(length2, offset);
    offset += 4;
    chunk.set(frame2, offset);
    offset += frame2.length;
    chunk.set(zero, offset);
    const result = assembler.push(chunk);
    // Multiple frames are concatenated into one message
    assertEquals(result, [new Uint8Array([...frame1, ...frame2])]);
  });

  it("push partial frame then complete", () => {
    const assembler = new FrameAssembler();
    const frame = new Uint8Array([1, 2, 3, 4, 5]);
    // Big-endian length prefix
    const length = new Uint8Array(4);
    new DataView(length.buffer).setUint32(0, frame.length, false);
    // First push: length header only
    const result1 = assembler.push(length);
    assertEquals(result1, []);
    // Second push: frame data completes the frame
    const result2 = assembler.push(frame);
    assertEquals(result2, []);
    // Third push: zero-length terminator flushes the message
    const result3 = assembler.push(new Uint8Array(4));
    assertEquals(result3, [frame]);
  });

  it("push frame with length 0 (flush)", () => {
    const assembler = new FrameAssembler();
    // Push a frame without terminator
    const frame = new Uint8Array([1, 2, 3]);
    const length = new Uint8Array(4);
    new DataView(length.buffer).setUint32(0, frame.length, false);
    const chunk1 = new Uint8Array(length.length + frame.length);
    chunk1.set(length, 0);
    chunk1.set(frame, 4);
    assembler.push(chunk1); // Frame collected but not flushed
    // Push zero-length to flush
    const zeroLength = new Uint8Array(4); // All zeros = length 0
    const result = assembler.push(zeroLength);
    assertEquals(result, [frame]);
  });

  it("push multiple frames then flush", () => {
    const assembler = new FrameAssembler();
    // Push two frames without terminator
    const frame1 = new Uint8Array([1]);
    const frame2 = new Uint8Array([2, 3]);
    const length1 = new Uint8Array(4);
    new DataView(length1.buffer).setUint32(0, 1, false);
    const length2 = new Uint8Array(4);
    new DataView(length2.buffer).setUint32(0, 2, false);
    assembler.push(new Uint8Array([...length1, ...frame1]));
    assembler.push(new Uint8Array([...length2, ...frame2]));
    // Flush with zero-length
    const result = assembler.push(new Uint8Array(4)); // length 0
    const expected = new Uint8Array([...frame1, ...frame2]); // Concatenated
    assertEquals(result, [expected]);
  });

  it("reset", () => {
    const assembler = new FrameAssembler();
    const frame = new Uint8Array([1, 2]);
    const length = new Uint8Array(4);
    new DataView(length.buffer).setUint32(0, 2, false);
    const zero = new Uint8Array(4);
    assembler.push(new Uint8Array([...length, ...frame])); // Partial push
    assembler.reset(); // Clear state
    // Push complete message after reset
    const result = assembler.push(
      new Uint8Array([...length, ...frame, ...zero]),
    );
    assertEquals(result, [frame]);
  });

  it("push chunk smaller than 4 bytes", () => {
    const assembler = new FrameAssembler();
    const result = assembler.push(new Uint8Array([1, 2, 3])); // Can't read length
    assertEquals(result, []);
  });

  it("push chunk with incomplete frame", () => {
    const assembler = new FrameAssembler();
    const length = new Uint8Array(4);
    new DataView(length.buffer).setUint32(0, 10, false); // Frame length 10
    const partialFrame = new Uint8Array([1, 2, 3]);
    const chunk = new Uint8Array([...length, ...partialFrame]);
    const result = assembler.push(chunk);
    assertEquals(result, []); // Not enough data for full frame
    // Complete the frame
    const remaining = new Uint8Array(7); // 10 - 3 bytes
    const result2 = assembler.push(remaining);
    assertEquals(result2, []); // Frame collected, but not flushed
    // Flush the message
    const result3 = assembler.push(new Uint8Array(4)); // Zero-length terminator
    const fullFrame = new Uint8Array(10);
    fullFrame.set(partialFrame, 0);
    fullFrame.set(remaining, 3);
    assertEquals(result3, [fullFrame]);
  });

  it("multiple flushes", () => {
    const assembler = new FrameAssembler();
    // First message: frame1 + flush
    const frame1 = new Uint8Array([1]);
    assembler.push(new Uint8Array([0, 0, 0, 1, 1])); // Length 1 + data
    let result = assembler.push(new Uint8Array(4)); // Flush
    assertEquals(result, [frame1]);
    // Second message: frame2 + flush
    const frame2 = new Uint8Array([2, 3]);
    assembler.push(new Uint8Array([0, 0, 0, 2, 2, 3])); // Length 2 + data
    result = assembler.push(new Uint8Array(4)); // Flush
    assertEquals(result, [frame2]);
  });

  it("empty flush", () => {
    const assembler = new FrameAssembler();
    const result = assembler.push(new Uint8Array(4)); // Zero-length with no frames
    assertEquals(result, [new Uint8Array(0)]); // Returns empty message
  });
});
