// recon-symphonym-quantise.js
// int8 quantisation of a Symphonym embedding, matching the writer that produced the stored vectors
// (indexing repo, update_es.quantize_embeddings_to_bytes): `np.round(unit_vector * 127.0)` cast to
// int8. Kept in its own module so the golden-fixture check exercises the function that ships rather
// than a copy of it.
//
// Two details that are easy to get wrong and were both wrong here:
//
//   NO CLIPPING. The index's writer does not clip, so neither may this. The embedding is
//   L2-normalised, so |v| <= 1 and round(v*127) can never leave [-127, 127] anyway — a clip is
//   dead code that quietly asserts a different contract from the one the index was written under.
//
//   np.round IS ROUND-HALF-TO-EVEN, and Math.round is not: Math.round(2.5) is 3 where np.round(2.5)
//   is 2, and Math.round(-2.5) is -2 where np.round(-2.5) is also -2 (Math.round breaks ties toward
//   +Infinity, numpy toward even). An exact .5 is rare in a float32 embedding but it is reachable,
//   and a single component off by one is a different vector.

function roundHalfToEven(x) {
  const floor = Math.floor(x);
  const diff = x - floor;
  if (diff > 0.5) return floor + 1;
  if (diff < 0.5) return floor;
  return floor % 2 === 0 ? floor : floor + 1;
}

// Write 128 quantised components of `emb` into `out` at offset `off`.
export function quantiseByte(emb, out, off) {
  for (let k = 0; k < 128; k++) out[off + k] = roundHalfToEven(emb[k] * 127.0);
}

export { roundHalfToEven };
