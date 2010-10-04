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

(** Number attribute type *)
type num = int32

(** Maximum network packet size *)
val max_packet_size : int32

(** searchd status codes *)
type status = Ok | Error | Retry | Warning 
val show_status : status -> string

(** match modes *)
type matching = MATCH_ALL | MATCH_ANY | MATCH_PHRASE | MATCH_BOOLEAN | MATCH_EXTENDED | MATCH_FULLSCAN | MATCH_EXTENDED2
val show_matching : matching -> string

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
val show_ranking : ranking -> string

(** sort modes *)
type sort = SORT_RELEVANCE | SORT_ATTR_DESC | SORT_ATTR_ASC | SORT_TIME_SEGMENTS | SORT_EXTENDED | SORT_EXPR
val show_sort : sort -> string

(** filter types *)
type filter = FILTER_VALUES | FILTER_RANGE | FILTER_FLOATRANGE
val show_filter : filter -> string

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

type attr_type = attr1 * bool (* type, MVA *)
val show_attr_type : attr_type -> string

type attr_value = F of float | L of num | Q of int64 | MVA of num array | S of string
val show_attr : attr_value -> string

(** grouping functions *)
type grouping = GROUPBY_DAY | GROUPBY_WEEK | GROUPBY_MONTH | GROUPBY_YEAR | GROUPBY_ATTR | GROUPBY_ATTRPAIR
val show_grouping : grouping -> string

type query =
  {
    mutable offset : int; (** how much records to seek from result-set start (default is 0) *)
    mutable limit : int; (** how much records to return from result-set starting at offset (default is 20) *)
    mutable mode : matching; (** query matching mode (default is MATCH_ALL) *)
    mutable sort : sort; (** match sorting mode (default is SORT_RELEVANCE) *)
    mutable sortby : string; (** attribute to sort by (default is "") *)
    mutable min_id : int64; (** min ID to match (default is 0) *)
    mutable max_id : int64; (** max ID to match (default is UINT_MAX) *)
    mutable filters : int list; (** search filters *)
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

val default : unit -> query

exception Fail of string

val parse_sockaddr : string -> Unix.sockaddr

type conn = private Unix.file_descr

(** [connect ?addr ?persist ()]
  @param addr searchd socket (default [127.0.0.1:9312])
  @param persist persistent connection (default [false] - connection is closed by the server after the first request)
*)
val connect : ?addr:Unix.sockaddr -> ?persist:bool -> unit -> conn

(** Close connection *)
val close : conn -> unit

(** Set offset and count into result set, and optionally set max-matches and cutoff limits. *)
val set_limits : query -> ?maxmatches:int -> ?cutoff:int -> offset:int -> limit:int -> unit

(** Set IDs range to match.
    Only match records if document ID is beetwen [id1] and [id2] (inclusive). *)
val set_id_range : query -> int64 -> int64 -> unit

(** build query packet *)
val build_query : query -> ?index:string -> ?comment:string -> string -> string

(** run queries batch *)
val run_queries : conn -> string list -> [> `Err of string | `Ok of result ] list * string option

(** Perform search query
    @raise Fail on protocol errors
    @return result set
*)
val query :
  conn ->
  query ->
  ?index:string ->
  ?comment:string ->
  string -> [> `Err of string | `Ok of result ] * string option

(** Flush attributes to disk *)
val flush_attrs : conn -> int32

(**
  [update_attrs conn index attrs values]
  updates given attribute values on given documents in given indexes.
  @return the number of updated documents (0 or more) on success, or -1 on failure

  @param attrs attribute names
  @param values document ids with corresponding new attribute values

  E.g.: [ update_attrs conn "test_index" ["id"; "n"] [2L,[123;1000]; 4L,[456;2000]; 5L,[789;3000]] ]
*)
val update_attrs : conn -> string -> string list -> (int64 * int list) list -> int * string option

