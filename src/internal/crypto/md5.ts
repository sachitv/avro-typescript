const S: number[] = [
  7,
  12,
  17,
  22,
  7,
  12,
  17,
  22,
  7,
  12,
  17,
  22,
  7,
  12,
  17,
  22,
  5,
  9,
  14,
  20,
  5,
  9,
  14,
  20,
  5,
  9,
  14,
  20,
  5,
  9,
  14,
  20,
  4,
  11,
  16,
  23,
  4,
  11,
  16,
  23,
  4,
  11,
  16,
  23,
  4,
  11,
  16,
  23,
  6,
  10,
  15,
  21,
  6,
  10,
  15,
  21,
  6,
  10,
  15,
  21,
  6,
  10,
  15,
  21,
];

const K = new Uint32Array(64);
for (let i = 0; i < 64; i++) {
  K[i] = Math.floor(Math.abs(Math.sin(i + 1)) * 2 ** 32);
}

const textEncoder = new TextEncoder();

export function md5FromString(value: string): Uint8Array {
  return md5(textEncoder.encode(value));
}

export function md5(input: Uint8Array): Uint8Array {
  const data = pad(input);

  let a = 0x67452301;
  let b = 0xefcdab89;
  let c = 0x98badcfe;
  let d = 0x10325476;

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);

  for (let offset = 0; offset < data.byteLength; offset += 64) {
    const chunk = new Uint32Array(16);
    for (let i = 0; i < 16; i++) {
      chunk[i] = view.getUint32(offset + i * 4, true);
    }

    let A = a;
    let B = b;
    let C = c;
    let D = d;

    for (let i = 0; i < 64; i++) {
      let F: number;
      let g: number;
      if (i < 16) {
        F = (B & C) | (~B & D);
        g = i;
      } else if (i < 32) {
        F = (D & B) | (~D & C);
        g = (5 * i + 1) % 16;
      } else if (i < 48) {
        F = B ^ C ^ D;
        g = (3 * i + 5) % 16;
      } else {
        F = C ^ (B | ~D);
        g = (7 * i) % 16;
      }

      const temp = D;
      D = C;
      C = B;
      const sum = (A + F + K[i] + chunk[g]) >>> 0;
      const rotated = rotateLeft(sum, S[i]);
      B = (B + rotated) >>> 0;
      A = temp;
    }

    a = (a + A) >>> 0;
    b = (b + B) >>> 0;
    c = (c + C) >>> 0;
    d = (d + D) >>> 0;
  }

  const result = new Uint8Array(16);
  const resultView = new DataView(result.buffer);
  resultView.setUint32(0, a, true);
  resultView.setUint32(4, b, true);
  resultView.setUint32(8, c, true);
  resultView.setUint32(12, d, true);
  return result;
}

function rotateLeft(value: number, shift: number): number {
  return ((value << shift) | (value >>> (32 - shift))) >>> 0;
}

function pad(input: Uint8Array): Uint8Array {
  const bitLength = BigInt(input.length) * 8n;
  let paddingLength = 56 - (input.length + 1) % 64;
  if (paddingLength < 0) {
    paddingLength += 64;
  }

  const totalLength = input.length + 1 + paddingLength + 8;
  const output = new Uint8Array(totalLength);
  output.set(input, 0);
  output[input.length] = 0x80;

  const view = new DataView(output.buffer);
  view.setUint32(totalLength - 8, Number(bitLength & 0xffffffffn), true);
  view.setUint32(
    totalLength - 4,
    Number((bitLength >> 32n) & 0xffffffffn),
    true,
  );

  return output;
}
