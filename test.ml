

let () =
  let q = Sphinx.default () in
  let c = Sphinx.connect () in
  let (r,w) = Sphinx.query c q "a b" in
  Option.may print_endline w;
  Bitstring.hexdump_bitstring stdout r;
  ()

