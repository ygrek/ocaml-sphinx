(*
  Sphinx searchd client in OCaml

  Copyright (c) 2010, ygrek@autistici.org

  This library is free software; you can redistribute it and/or
  modify it under the terms of the GNU Lesser General Public
  License as published by the Free Software Foundation; either
  version 2.1 of the License, or (at your option) any later version.

  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public
  License along with this library; if not, write to the Free Software
  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
*)

(**
  {{:http://sphinxsearch.com/}Sphinx} API
  @author ygrek
*)

open ExtLib
open Unix
open Bitstring
open Printf

(** searchd commands and implementation versions *)
module Command = struct
let search = (0, 0x117)
(* let excerpt = (1, 0x100) *)
let update = (2, 0x102)
(* let keywords = (3, 0x100) *)
let persist = (4, 0)
(*
let status = (5, 0x100)
let query = (6, 0x100)
*)
let flushattrs = (7, 0x100)
end

(** Number attribute type *)
(*
type num = int
let string_of_num = string_of_int
let read_num = IO.BigEndian.read_i32
*)
type num = int32
let string_of_num x = Int32.to_string x
let read_num = IO.BigEndian.read_real_i32

(** Maximum network packet size, 2 MB is enough (c) *)
let max_packet_size = Int32.of_int (2 * 1024 * 1024)

(** searchd status codes *)
type status = Ok | Error | Retry | Warning 
  deriving (Enum,Show)

let show_status = Show.show<status>

(** match modes *)
type matching = MATCH_ALL | MATCH_ANY | MATCH_PHRASE | MATCH_BOOLEAN | MATCH_EXTENDED | MATCH_FULLSCAN | MATCH_EXTENDED2
  deriving (Enum,Show)

let show_matching = Show.show<matching>

(** ranking modes (for MATCH_EXTENDED2 only) *)
type ranking =
  | RANK_PROXIMITY_BM25 (** default mode, phrase proximity major factor and BM25 minor one *)
  | RANK_BM25 (** statistical mode, BM25 ranking only (faster but worse quality) *)
  | RANK_NONE (** no ranking, all matches get a weight of 1 *)
  | RANK_WORDCOUNT (** simple word-count weighting, rank is a weighted sum of per-field keyword occurence counts *)
  | RANK_PROXIMITY
  | RANK_MATCHANY
  | RANK_FIELDMASK
  | RANK_SPH04
  deriving (Enum,Show)

let show_ranking = Show.show<ranking>

(** sort modes *)
type sort = SORT_RELEVANCE | SORT_ATTR_DESC | SORT_ATTR_ASC | SORT_TIME_SEGMENTS | SORT_EXTENDED | SORT_EXPR
  deriving (Enum,Show)

let show_sort = Show.show<sort>

(** filter types *)
type filter = FILTER_VALUES of int64 array | FILTER_RANGE of int64 * int64 | FILTER_FLOATRANGE of float * float
  deriving (Show)

let show_filter = Show.show<filter>

(** attribute types *)
type attr1 = 
  | ATTR_NONE (** not an attribute (unknown) *)
  | ATTR_INTEGER
  | ATTR_TIMESTAMP 
  | ATTR_ORDINAL (** ordinal string number *)
  | ATTR_BOOL (** boolean bit field *)
  | ATTR_FLOAT
  | ATTR_BIGINT
  | ATTR_STRING
  | ATTR_WORDCOUNT (** string word count *)
  deriving (Enum,Show)

type attr_type = attr1 * bool

let show_attr_type = function
  | (t,true) -> sprintf "MVA(%s)" (Show.show<attr1>(t))
  | (t,false) -> Show.show<attr1>(t)

type attr_value = F of float | L of num | Q of int64 | MVA of num array | S of string

let show_attr = function
  | F f -> string_of_float f
  | S s -> sprintf "%S" s
  | L x -> string_of_num x
  | Q x -> Int64.to_string x
  | MVA l -> "[" ^ String.concat "," (List.map string_of_num (Array.to_list l)) ^ "]"

let attr_multi = 0X40000000l
let attr_of_int32 a =
  let multi = Int32.logand a attr_multi = attr_multi in
  try Enum.to_enum<attr1>((Int32.to_int a) land 0x00FFFFFF), multi with _ -> ATTR_NONE, multi

(** grouping functions *)
type grouping = GROUPBY_DAY | GROUPBY_WEEK | GROUPBY_MONTH | GROUPBY_YEAR | GROUPBY_ATTR | GROUPBY_ATTRPAIR
  deriving (Enum,Show)

let show_grouping = Show.show<grouping>

type query =
  {
    mutable offset : int; (** how much records to seek from result-set start (default is 0) *)
    mutable limit : int; (** how much records to return from result-set starting at offset (default is 20) *)
    mutable mode : matching; (** query matching mode (default is MATCH_ALL) *)
    mutable sort : sort; (** match sorting mode (default is SORT_RELEVANCE) *)
    mutable sortby : string; (** attribute to sort by (default is "") *)
    mutable min_id : int64; (** min ID to match (default is 0) *)
    mutable max_id : int64; (** max ID to match (default is UINT_MAX) *)
    mutable filters : (string * filter * bool) list; (** search filters : attribute * filter * exclude *)
    mutable groupby : string; (** group-by attribute name *)
    mutable groupfunc : grouping; (** group-by function (to pre-process group-by attribute value with) *)
    mutable groupsort  : string; (** group-by sorting clause (to sort groups in result set with) *)
    mutable groupdistinct : string; (** group-by count-distinct attribute *)
    mutable maxmatches : int; (** max matches to retrieve (default is 1000) *)
    mutable cutoff : int; (** cutoff to stop searching at *)
    mutable retrycount : int; (** distributed retry count *)
    mutable retrydelay : int; (** distributed retry delay *)
    mutable anchor : int list; (** geographical anchor point *)
    mutable indexweights : (string * int) list; (** per-index weights *)
    mutable ranker : ranking; (** ranking mode (default is RANK_PROXIMITY_BM25) *)
    mutable maxquerytime : int; (** max query time, milliseconds (default is 0, do not limit) *)
    mutable fieldweights : (string * int) list; (** per-field-name weights (default is 1 for all fields) *)
    mutable overrides : int list; (** per-query attribute values overrides *)
    mutable select : string; (** select-list (attributes or expressions, with optional aliases) *)
  }

type result =
  { 
    fields : string array; (* schema (field names) *)
    attrs : string array; (* attribute names *)
    matches : (int64 * int * attr_value array) array; (** matches: (document id, weight, attributes) *)
    total : int;
    total_found : int;
    time : int; (** query execution time, in milliseconds *)
    words : (string * (int * int)) array; (** words statistics: (word, (documents, hits)) *)
    warning : string option; (** warning message *)
  }

let default () = 
  {
    offset = 0;
    limit = 20;
    mode = MATCH_ALL;
    sort = SORT_RELEVANCE;
    sortby = "";
    min_id = 0L;
    max_id = 0L;
    filters = [];
    groupby = "";
    groupfunc = GROUPBY_DAY;
    groupsort = "@group desc";
    groupdistinct = "";
    maxmatches = 1000;
    cutoff = 0;
    retrycount = 0;
    retrydelay = 0;
    anchor = [];
    indexweights = [];
    ranker = RANK_PROXIMITY_BM25;
    maxquerytime = 0;
    fieldweights = [];
    overrides = [];
    select = "*";
  }

exception Fail of string

let fail fmt = ksprintf (fun s -> raise (Fail s)) fmt

let recv sock n = 
  let s = String.create n in
  let n' = read sock s 0 n in
  if n = n' then s else fail "recv: expected %u bytes, but got %u" n n'

let send sock s =
  let n = String.length s in
  let n' = write sock s 0 n in
  if n <> n' then fail "send: expected %u bytes, but sent %u" n n'

let (&) f x = f x
let (>>) x f = f x
let bits = bitstring_of_string
let catch f x = try Some (f x) with _ -> None

let parse_sockaddr s =
  let inet s n = Unix.ADDR_INET (Unix.inet_addr_of_string s, n) in
  let unix s = Unix.ADDR_UNIX s in
  try Scanf.sscanf s "[%s@]:%u" inet with _ -> 
  try Scanf.sscanf s "%s@:%u" inet with _ ->
  try Scanf.sscanf s "unix://%s" unix with _ ->
  if String.starts_with s "/" then unix s else fail "unrecognized socket address : %s" s

type conn = Unix.file_descr

(** [connect ?addr ?persist ()]
  @param addr searchd socket (default [127.0.0.1:9312])
  @param persist persistent connection (default [false] - connection is closed by the server after the first request)
*)
let connect ?(addr=ADDR_INET(inet_addr_loopback,9312)) ?(persist=false) () =
  let sock = socket PF_INET SOCK_STREAM 0 in
  try
    connect sock addr;
    let () = bitmatch bits & recv sock 4 with (* check server version *)
    | { v : 32 } when v >= 1l -> ()
    | { s : -1 : string } -> fail "expected searchd version, got %S" s
    in
    send sock & string_of_bitstring (BITSTRING { 1l : 32 }); (* client version *)

    if persist then
      send sock & string_of_bitstring (BITSTRING { fst Command.persist : 16; snd Command.persist : 16; 4l : 32; 1l : 32 });

    sock
  with
    exn -> close sock; raise exn

let close = close

let get_response sock client_ver =
  bitmatch bits & recv sock 8 with
  | { status : 16; version : 16; length : 32 } ->
    let () = assert (length < max_packet_size) in
    let length = Int32.to_int length in
    let r = String.create length in
    let cur = ref 0 in
    let () = while !cur < length do
      match read sock r !cur (length - !cur) with
      | 0 -> close sock; fail "get_response %u %u" length !cur
      | n -> cur := !cur + n
    done in

    begin match catch Enum.to_enum<status>(status) with
    | None -> fail "get_response: unknown status code %d" status
    | Some Error -> fail "get_response: error: %S" (String.slice ~first:4 r)
    | Some Retry -> fail "get_response: temporary error: %S" (String.slice ~first:4 r)
    | Some Ok ->
      let w = if version < client_ver then
        Some (sprintf "get_response: searchd command v.%d.%d older than client's v.%d.%d, some options might not work"
                  (version lsr 8) (version land 0xFF) (client_ver lsr 8) (client_ver land 0xFF))
      else None 
      in
      r, w
    | Some Warning ->
      bitmatch bits r with
      | { len : 32; w : (Int32.to_int len) : string; r : -1 : string } -> (r, Some w)
      | { _ } -> fail "get_response: bad warning: %S" r
    end
  | { s : -1 : string } -> fail "get_response: recv: %S" s

(** Set offset and count into result set, and optionally set max-matches and cutoff limits. *)
let set_limits q ?maxmatches ?cutoff ~offset ~limit =
    assert (0 <= offset && offset < 16777216);
    assert (0 < limit && offset < 16777216);
    q.offset <- offset;
    q.limit <- limit;
    begin match maxmatches with Some n when n > 0 -> q.maxmatches <- n | _ -> () end;
    match cutoff with Some n when n >= 0 -> q.cutoff <- n | _ -> ()

(** Set IDs range to match.
    Only match records if document ID is beetwen [id1] and [id2] (inclusive). *)
let set_id_range q id1 id2 =
    q.min_id <- min id1 id2;
    q.max_id <- max id1 id2

let add_filter q a f exclude =
    q.filters <- (a,f,exclude) :: q.filters

let dw = IO.BigEndian.write_i16
let dd = IO.BigEndian.write_i32
let dq = IO.BigEndian.write_i64
let df out f = IO.BigEndian.write_real_i32 out (Int32.bits_of_float f)
let str out s = dd out (String.length s); IO.nwrite out s
let pair kx ky (x,y) = kx x; ky y
let list out l k = dd out (List.length l); List.iter k l
let array out a k = dd out (Array.length a); Array.iter k a

(** build query packet *)
let build_query q ?(index="*") ?(comment="") query =
  let out = IO.output_string () in
  let dd = dd out and dq = dq out and str = str out and list l = list out l and df = df out in

  dd q.offset;
  dd q.limit;
  dd & Enum.from_enum<matching> q.mode;
  dd & Enum.from_enum<ranking> q.ranker;
  dd & Enum.from_enum<sort> q.sort;
  str q.sortby;
  str query;
  dd 0; (* weights, deprecated, empty list *)
  str index;
  dd 1; (* id64 range marker *)
  dq q.min_id;
  dq q.max_id;
  list q.filters (fun (attr,f,exclude) ->
    str attr;
    begin match f with
    | FILTER_VALUES a -> dd 0; array out a dq
    | FILTER_RANGE (q1,q2) -> dd 1; dq q1; dq q2
    | FILTER_FLOATRANGE (f1,f2) -> dd 2; df f1; df f2
    end;
    dd (if exclude then 1 else 0));

  dd & Enum.from_enum<grouping> q.groupfunc;
  str q.groupby;
  dd q.maxmatches;
  str q.groupsort;
  dd q.cutoff;
  dd q.retrycount;
  dd q.retrydelay;
  str q.groupdistinct;

  (* TODO anchor
      attrlat, attrlong = self._anchor['attrlat'], self._anchor['attrlong']
      latitude, longitude = self._anchor['lat'], self._anchor['long']
      req.append ( pack ('>L', 1))
      req.append ( pack ('>L', len(attrlat)) + attrlat)
      req.append ( pack ('>L', len(attrlong)) + attrlong)
      req.append ( pack ('>f', latitude) + pack ('>f', longitude))
  *)
  dd 0;

  list q.indexweights (pair str dd);
  dd q.maxquerytime;
  list q.fieldweights (pair str dd);
  str comment;

  (* TODO attribute overrides
    req.append ( pack('>L', len(self._overrides)) )
    for v in self._overrides.values():
      req.extend ( ( pack('>L', len(v['name'])), v['name'] ) )
      req.append ( pack('>LL', v['type'], len(v['values'])) )
      for id, value in v['values'].iteritems():
        req.append ( pack('>Q', id) )
        if v['type'] == SPH_ATTR_FLOAT:
          req.append ( pack('>f', value) )
        elif v['type'] == SPH_ATTR_BIGINT:
          req.append ( pack('>q', value) )
        else:
          req.append ( pack('>l', value) )
  *)
  list [] dd;

  str q.select;

  IO.close_out out

(* let pr fmt = ksprintf prerr_endline fmt *)

(** run queries batch *)
let run_queries sock reqs =
  assert (reqs <> []);

  let () = 
    let out = IO.output_string () in
    let len = List.fold_left (fun acc s -> acc + String.length s) 0 reqs in

    dw out (fst Command.search);
    dw out (snd Command.search);
    dd out (len + 4);
    list out reqs (IO.nwrite out);

    send sock (IO.close_out out);
  in

  let (r,w) = get_response sock (snd Command.search) in

  let cin = IO.input_string r in
  let long () = IO.BigEndian.read_i32 cin in
  let num () = read_num cin in
  let qword () = IO.BigEndian.read_i64 cin in
  let float () = Int32.float_of_bits (IO.BigEndian.read_real_i32 cin) in
  let str () = let len = long () in IO.really_nread cin len in
  let list k = let len = long () in Array.init len (fun _ -> k ()) in
  let pair kx ky = fun () -> let x = kx () in let y = ky () in (x,y) in

  let result warning =
    let fields = list str in
    let attrs = list (pair str (fun () -> IO.BigEndian.read_real_i32 cin)) in
    let count = long () in
    let id64 = long () in
    let matches = Array.init count begin fun _ ->
      let doc = if id64 > 0 then qword () else Int64.of_int (long ()) in
      let weight = long () in
      let attrs = attrs >> Array.map begin fun (_,t) ->
        match attr_of_int32 t with
        | ATTR_STRING, false -> S (str ())
        | ATTR_FLOAT, false -> F (float ())
        | ATTR_BIGINT, false -> Q (qword ())
        | (ATTR_NONE | ATTR_STRING | ATTR_FLOAT | ATTR_BIGINT), true -> fail "unsupported MVA type : 0x%lX" t
        | _, true -> MVA (list num)
        | _, false -> L (num ())
        end
      in
      (doc,weight,attrs)
    end
    in
    let attrs = Array.map fst attrs in
    let total = long () in
    let total_found = long () in
    let time = long () in
    let words = list (pair str (pair long long)) in
    { fields = fields; attrs = attrs; matches = matches; total = total; total_found = total_found; time = time; words = words; warning = warning }
  in
  let get () =
    let st = long () in
    match catch Enum.to_enum<status>(st) with
    | None -> fail "run_queries: unknown status code %d" st
    | Some Error | Some Retry -> `Err (str ())
    | Some Warning -> let warn = Some (str ()) in `Ok (result warn)
    | Some Ok -> `Ok (result None)
  in
  List.map (fun _ -> get ()) reqs, w

(** Perform search query
    @raise Fail on protocol errors
    @return result set
*)
let query sock q ?index ?comment s =
  let s = build_query q ?index ?comment s in
  match run_queries sock [s] with
  | [r], w -> r, w
  | _ -> fail "query"

let flush_attrs sock =
  send sock & string_of_bitstring (BITSTRING { fst Command.flushattrs : 16; snd Command.flushattrs : 16; 0l : 32 });
  let (r,w) = get_response sock (snd Command.flushattrs) in
  bitmatch bits r with
  | { tag : 32 } -> tag
  | { s : -1 : string } -> fail "flush_attrs: unexpected response : %S" s

(**
  [update_attrs conn index attrs values]
  updates given attribute values on given documents in given indexes.
  @return the number of updated documents (0 or more) on success, or -1 on failure

  @param attrs attribute names
  @param values document ids with corresponding new attribute values

  E.g.: [ update_attrs conn "test_index" ["id"; "n"] [2L,[123;1000]; 4L,[456;2000]; 5L,[789;3000]] ]
*)
let update_attrs sock index attrs values =
  let out = IO.output_string () in
  let str = str out and dd = dd out in
  let n = List.length attrs in
  str index;
  list out attrs (fun s -> str s; dd 0 (* ordinary attribute, not MVA *));
  list out values (fun (docid, attrs) ->
    if n <> List.length attrs then invalid_arg (sprintf "update_attrs(%s): not enough attributes for docid %Ld" index docid);
    dq out docid; List.iter dd attrs);
  let pkt = IO.close_out out in
  send sock & string_of_bitstring (BITSTRING { fst Command.update : 16; snd Command.update : 16; Int32.of_int (String.length pkt) : 32 });
  send sock pkt;
  let (r,w) = get_response sock (snd Command.update) in
  bitmatch bits r with
  | { n : 32 } -> Int32.to_int n, w
  | { s : -1 : string } -> fail "update_attrs: unexpected response : %S" s

