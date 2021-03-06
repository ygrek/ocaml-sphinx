
open Printf
open ExtLib
open Sphinx

let pr fmt = ksprintf print_endline fmt

let array k a = String.concat "," (Array.to_list (Array.map k a))
let id x = x

let search ?addr ?index queries =
  let q = default () in
  let c = connect ?addr ~persist:true () in
  q.mode <- MATCH_EXTENDED2;
  let each s =
    pr "Query : %s" s;
    try
      let (r,warning) = query c q ?index s in
      Option.may (pr "Server warning : %s") warning;
      Option.may (pr "Query warning : %s") r.warning;
      pr "Fields : [%s]" (array id r.fields);
      pr "Attributes : [%s]" (array id r.attrs);
      pr "Total %d, total_found %d, time %d ms" r.total r.total_found r.time;
      Array.iteri (fun i (id,weight,attrs) -> pr "%d) doc %Ld weight %d attrs [%s]" i id weight (array show_attr attrs)) r.matches;
      pr "words:";
      Array.iter (fun (word,(docs,hits)) -> pr "%s (docs %d, hits %d)" word docs hits) r.words;
      pr ""
    with
    | Fail s -> pr "Error : %s" s
    | exn -> pr "Exception : %s" (Printexc.to_string exn)
  in
  List.iter each queries;
  close c

let () =
  let index = ref None in
  let addr = ref None in
  let queries = ref [] in
  let rec loop = function
  | "-i"::s::l -> index := Some s; loop l
  | "-c"::s::l -> addr := Some (parse_sockaddr s); loop l
  | s::l -> queries := s :: !queries; loop l
  | [] -> search ?addr:!addr ?index:!index !queries
  in
  match Array.to_list Sys.argv with
  | [] -> assert false
  | [name] -> pr "%s [-i <index>] [-c <address>] <query>" name
  | _::l -> loop l 

