
open Printf
open ExtLib
open Sphinx

let pr fmt = ksprintf print_endline fmt

let search ?index squery =
  let q = default () in
  let c = connect () in
  q.mode <- MATCH_EXTENDED2;
  let r = query c q ?index squery in
  close c;
  Option.may (pr "Warning : %s") (snd r);
  match fst r with
  | `Err s -> pr "Error : %s" s
  | `Ok (r,w) -> 
    Option.may (pr "warning : %s") w;
    pr "Fields : [%s]" (String.concat "," r.fields);
    pr "Total %d, total_found %d, time %d ms" r.total r.total_found r.time;
    List.iteri (fun i (id,weight,attrs) -> pr "%d) doc %Ld weight %d attrs [%s]" i id weight (String.concat "," (List.map fst attrs))) r.matches;
    pr "words:";
    List.iter (fun (word,(docs,hits)) -> pr "%s (docs %d, hits %d)" word docs hits) r.words;
    pr ""

let () =
  match Array.to_list Sys.argv with
  | _::q::[] -> search q
  | _::index::q::[] -> search ~index q
  | [name] -> pr "%s [index] query" name
  | _ -> assert false

