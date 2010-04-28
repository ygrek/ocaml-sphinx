
open Printf
open ExtLib
open Sphinx

let pr fmt = ksprintf print_endline fmt

let search ?addr ?index queries =
  let q = default () in
  let c = connect ?addr ~persist:true () in
  q.mode <- MATCH_EXTENDED2;
  let each s =
    pr "Query : %s" s;
    let r = query c q ?index s in
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

