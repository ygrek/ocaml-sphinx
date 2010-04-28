(**
  Sphinx searchd client in OCaml

  Based on sphinxapi.py r2218

  Copyright (c) 2010, ygrek@autistici.org

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License. You should have
  received a copy of the GPL license along with this program; if you
  did not, you can find it at http://www.gnu.org/
*)

open ExtLib
open Unix
open Bitstring
open Printf

(** searchd commands and implementation versions *)
module Command = struct
let search = (0, 0x116)
let excerpt = (1, 0x100)
let update = (2, 0x101)
let keywords = (3, 0x100)
let persist = (4, 0)
let flushattrs = (7, 0x100)
end

(** searchd status codes *)
type status = Ok | Error | Retry | Warning 
  deriving (Enum)

(** match modes *)
type matching = MATCH_ALL | MATCH_ANY | MATCH_PHRASE | MATCH_BOOLEAN | MATCH_EXTENDED | MATCH_FULLSCAN | MATCH_EXTENDED2
  deriving (Enum)

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
  | RANK_TOTAL
  deriving (Enum)

(** sort modes *)
type sort = SORT_RELEVANCE | SORT_ATTR_DESC | SORT_ATTR_ASC | SORT_TIME_SEGMENTS | SORT_EXTENDED | SORT_EXPR
  deriving (Enum)

(** filter types *)
type filter = FILTER_VALUES | FILTER_RANGE | FILTER_FLOATRANGE
  deriving (Enum)

(** attribute types *)
type attr1 = ATTR_NONE | ATTR_INTEGER | ATTR_TIMESTAMP | ATTR_ORDINAL | ATTR_BOOL | ATTR_FLOAT | ATTR_BIGINT
  deriving (Enum)
type attr = attr1 * bool
type attr_value = F of float | L of int | Q of int64 | LI of int list

let attr_multi = 0X40000000
let attr_of_int a =
  let multi = a land attr_multi = attr_multi in
  try Enum.to_enum<attr1>(a land (lnot attr_multi)), multi with _ -> ATTR_NONE, multi

(** grouping functions *)
type grouping = GROUPBY_DAY | GROUPBY_WEEK | GROUPBY_MONTH | GROUPBY_YEAR | GROUPBY_ATTR | GROUPBY_ATTRPAIR
  deriving (Enum)

type query =
  {
    mutable offset : int; (** how much records to seek from result-set start (default is 0) *)
    mutable limit : int; (** how much records to return from result-set starting at offset (default is 20) *)
    mutable mode : matching; (** query matching mode (default is SPH_MATCH_ALL) *)
    mutable sort : sort; (** match sorting mode (default is SPH_SORT_RELEVANCE) *)
    mutable sortby : string; (** attribute to sort by (defualt is "") *)
		mutable min_id : int64; (** min ID to match (default is 0) *)
		mutable max_id : int64; (** max ID to match (default is UINT_MAX) *)
		mutable filters : int list; (** search filters *)
		mutable groupby : string; (** group-by attribute name *)
		mutable groupfunc : grouping; (** group-by function (to pre-process group-by attribute value with) *)
		mutable groupsort	: string; (** group-by sorting clause (to sort groups in result set with) *)
		mutable groupdistinct : string; (** group-by count-distinct attribute *)
		mutable maxmatches : int; (** max matches to retrieve (default is 1000) *)
		mutable cutoff : int; (** cutoff to stop searching at *)
		mutable retrycount : int; (** distributed retry count *)
		mutable retrydelay : int; (** distributed retry delay *)
		mutable anchor : int list; (** geographical anchor point *)
		mutable indexweights : (string * int) list; (** per-index weights *)
		mutable ranker : ranking; (** ranking mode *)
		mutable maxquerytime : int; (** max query time, milliseconds (default is 0, do not limit) *)
		mutable fieldweights : (string * int) list; (** per-field-name weights (default is 1 for all fields) *)
		mutable overrides : int list; (** per-query attribute values overrides *)
		mutable select : string; (** select-list (attributes or expressions, with optional aliases) *)
  }

type result =
  { 
    fields : string list;
    matches : (int64 * int * (string * attr_value) list) list;
    total : int;
    total_found : int;
    time : int; (** milliseconds *)
    words : (string * (int * int)) list; (** word * (docs * hits) *)
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

(*
		if self._socket:
			# we have a socket, but is it still alive?
			sr, sw, _ = select.select ( [self._socket], [self._socket], [], 0 )

			# this is how alive socket should look
			if len(sr)==0 and len(sw)==1:
				return self._socket

			# oops, looks like it was closed, lets reopen
			self._socket.close()
			self._socket = None
*)

let fail fmt = ksprintf failwith fmt

let recv sock n = 
  let s = String.create n in
  let n' = recv sock s 0 n [] in
  if n = n' then s else fail "recv: expected %u bytes, but got %u" n n'

let send sock s =
  let n = String.length s in
  let n' = send sock s 0 n [] in
  if n <> n' then fail "send: expected %u bytes, but sent %u" n n'

let (&) f x = f x
let (>>) x f = f x
let bits = bitstring_of_string
let catch f x = try Some (f x) with _ -> None

let connect ?(addr=ADDR_INET(inet_addr_loopback,9312)) () =
	let sock = socket PF_INET SOCK_STREAM 0 in
  try
    connect sock addr;
    let () = bitmatch bits & recv sock 4 with
    | { v : 32 } when v >= 1l -> ()
    | { s : -1 : string } -> fail "expected searchd version, got %S" s
    in
		send sock & string_of_bitstring (BITSTRING { 1l : 32 });
		sock
  with
    exn -> close sock; raise exn

let close = close

let get_response sock client_ver =
  bitmatch bits & recv sock 8 with
  | { status : 16; version : 16; length : 32 } ->
    let length = Int32.to_int length (* FIXME check overflow? *) in
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

(*
	def SetMaxQueryTime (self, maxquerytime):
		"""
		Set maximum query time, in milliseconds, per-index. 0 means 'do not limit'.
		"""
		assert(isinstance(maxquerytime,int) and maxquerytime>0)
		self._maxquerytime = maxquerytime


	def SetMatchMode (self, mode):
		"""
		Set matching mode.
		"""
		assert(mode in [SPH_MATCH_ALL, SPH_MATCH_ANY, SPH_MATCH_PHRASE, SPH_MATCH_BOOLEAN, SPH_MATCH_EXTENDED, SPH_MATCH_FULLSCAN, SPH_MATCH_EXTENDED2])
		self._mode = mode


	def SetRankingMode (self, ranker):
		"""
		Set ranking mode.
		"""
		assert(ranker>=0 and ranker<SPH_RANK_TOTAL)
		self._ranker = ranker


	def SetSortMode ( self, mode, clause='' ):
		"""
		Set sorting mode.
		"""
		assert ( mode in [SPH_SORT_RELEVANCE, SPH_SORT_ATTR_DESC, SPH_SORT_ATTR_ASC, SPH_SORT_TIME_SEGMENTS, SPH_SORT_EXTENDED, SPH_SORT_EXPR] )
		assert ( isinstance ( clause, str ) )
		self._sort = mode
		self._sortby = clause


	def SetWeights (self, weights): 
		"""
		Set per-field weights.
		WARNING, DEPRECATED; do not use it! use SetFieldWeights() instead
		"""
		assert(isinstance(weights, list))
		for w in weights:
			assert(isinstance(w, int))
		self._weights = weights


	def SetFieldWeights (self, weights):
		"""
		Bind per-field weights by name; expects (name,field_weight) dictionary as argument.
		"""
		assert(isinstance(weights,dict))
		for key,val in weights.items():
			assert(isinstance(key,str))
			assert(isinstance(val,int))
		self._fieldweights = weights


	def SetIndexWeights (self, weights):
		"""
		Bind per-index weights by name; expects (name,index_weight) dictionary as argument.
		"""
		assert(isinstance(weights,dict))
		for key,val in weights.items():
			assert(isinstance(key,str))
			assert(isinstance(val,int))
		self._indexweights = weights


	def SetIDRange (self, minid, maxid):
		"""
		Set IDs range to match.
		Only match records if document ID is beetwen $min and $max (inclusive).
		"""
		assert(isinstance(minid, (int, long)))
		assert(isinstance(maxid, (int, long)))
		assert(minid<=maxid)
		self._min_id = minid
		self._max_id = maxid


	def SetFilter ( self, attribute, values, exclude=0 ):
		"""
		Set values set filter.
		Only match records where 'attribute' value is in given 'values' set.
		"""
		assert(isinstance(attribute, str))
		assert iter(values)

		for value in values:
			assert(isinstance(value, int))

		self._filters.append ( { 'type':SPH_FILTER_VALUES, 'attr':attribute, 'exclude':exclude, 'values':values } )


	def SetFilterRange (self, attribute, min_, max_, exclude=0 ):
		"""
		Set range filter.
		Only match records if 'attribute' value is beetwen 'min_' and 'max_' (inclusive).
		"""
		assert(isinstance(attribute, str))
		assert(isinstance(min_, int))
		assert(isinstance(max_, int))
		assert(min_<=max_)

		self._filters.append ( { 'type':SPH_FILTER_RANGE, 'attr':attribute, 'exclude':exclude, 'min':min_, 'max':max_ } )


	def SetFilterFloatRange (self, attribute, min_, max_, exclude=0 ):
		assert(isinstance(attribute,str))
		assert(isinstance(min_,float))
		assert(isinstance(max_,float))
		assert(min_ <= max_)
		self._filters.append ( {'type':SPH_FILTER_FLOATRANGE, 'attr':attribute, 'exclude':exclude, 'min':min_, 'max':max_} ) 


	def SetGeoAnchor (self, attrlat, attrlong, latitude, longitude):
		assert(isinstance(attrlat,str))
		assert(isinstance(attrlong,str))
		assert(isinstance(latitude,float))
		assert(isinstance(longitude,float))
		self._anchor['attrlat'] = attrlat
		self._anchor['attrlong'] = attrlong
		self._anchor['lat'] = latitude
		self._anchor['long'] = longitude


	def SetGroupBy ( self, attribute, func, groupsort='@group desc' ):
		"""
		Set grouping attribute and function.
		"""
		assert(isinstance(attribute, str))
		assert(func in [SPH_GROUPBY_DAY, SPH_GROUPBY_WEEK, SPH_GROUPBY_MONTH, SPH_GROUPBY_YEAR, SPH_GROUPBY_ATTR, SPH_GROUPBY_ATTRPAIR] )
		assert(isinstance(groupsort, str))

		self._groupby = attribute
		self._groupfunc = func
		self._groupsort = groupsort


	def SetGroupDistinct (self, attribute):
		assert(isinstance(attribute,str))
		self._groupdistinct = attribute


	def SetRetries (self, count, delay=0):
		assert(isinstance(count,int) and count>=0)
		assert(isinstance(delay,int) and delay>=0)
		self._retrycount = count
		self._retrydelay = delay


	def SetOverride (self, name, type, values):
		assert(isinstance(name, str))
		assert(type in SPH_ATTR_TYPES)
		assert(isinstance(values, dict))

		self._overrides[name] = {'name': name, 'type': type, 'values': values}

	def SetSelect (self, select):
		assert(isinstance(select, str))
		self._select = select


	def ResetOverrides (self):
		self._overrides = {}


	def ResetFilters (self):
		"""
		Clear all filters (for multi-queries).
		"""
		self._filters = []
		self._anchor = {}


	def ResetGroupBy (self):
		"""
		Clear groupby settings (for multi-queries).
		"""
		self._groupby = ''
		self._groupfunc = SPH_GROUPBY_DAY
		self._groupsort = '@group desc'
		self._groupdistinct = ''
*)

(*
		self._error = results[0]['error']
		self._warning = results[0]['warning']
		if results[0]['status'] == SEARCHD_ERROR:
			return None
*)

(** build query packet *)
let build q ?(index="*") ?(comment="") query =
  let out = IO.output_string () in
  let dd = IO.BigEndian.write_i32 out in
  let dq = IO.BigEndian.write_i64 out in
  let str s = dd (String.length s); IO.nwrite out s in
  let list l k = dd (List.length l); List.iter k l in
  let pair kx ky (x,y) = kx x; ky y in

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
  (* TODO filters 
		req.append ( pack ( '>L', len(self._filters) ) )
		for f in self._filters:
			req.append ( pack ( '>L', len(f['attr'])) + f['attr'])
			filtertype = f['type']
			req.append ( pack ( '>L', filtertype))
			if filtertype == SPH_FILTER_VALUES:
				req.append ( pack ('>L', len(f['values'])))
				for val in f['values']:
					req.append ( pack ('>q', val))
			elif filtertype == SPH_FILTER_RANGE:
				req.append ( pack ('>2q', f['min'], f['max']))
			elif filtertype == SPH_FILTER_FLOATRANGE:
				req.append ( pack ('>2f', f['min'], f['max']))
			req.append ( pack ( '>L', f['exclude'] ) )
  *)
  list [] dd;

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

(** Run queries batch.
		@return an array of result sets *)
let run sock reqs =
  assert (reqs <> []);

  let () = 
    let out = IO.output_string () in
    let dw = IO.BigEndian.write_i16 out in
    let dd = IO.BigEndian.write_i32 out in
    let list l k = dd (List.length l); List.iter k l in
    let len = List.fold_left (fun acc s -> acc + String.length s) 0 reqs in

    dw & fst Command.search;
    dw & snd Command.search;
    dd & len + 4;
    list reqs (IO.nwrite out);

  	send sock & IO.close_out out;
  in

  let (r,w) = get_response sock (snd Command.search) in

  let cin = IO.input_string r in
  let long () = IO.BigEndian.read_i32 cin in
  let qword () = IO.BigEndian.read_i64 cin in
  let float () = Int32.float_of_bits (IO.BigEndian.read_real_i32 cin) in
  let str () = let len = long () in IO.really_nread cin len in
  let list k = let len = long () in List.init len (fun _ -> k ()) in
  let pair kx ky = fun () -> let x = kx () in let y = ky () in (x,y) in

  let result () =
    let fields = list str in
    let attrs = list (pair str long) in
    let count = long () in
    let id64 = long () in
    let matches = List.init count begin fun _ ->
      let doc = if id64 > 0 then qword () else Int64.of_int (long ()) in
      let weight = long () in
      let attrs = attrs >> List.map (fun (name,t) ->
        let v = match attr_of_int t with
        | ATTR_FLOAT, false -> F (float ())
        | ATTR_BIGINT, false -> Q (qword ())
        | ATTR_INTEGER, true -> LI (list long)
        | _, false -> L (long ())
        | _ -> fail "unsupported attribute type : 0x%X" t
        in
        name, v)
      in
      (doc,weight,attrs)
    end
    in
    let total = long () in
    let total_found = long () in
    let time = long () in
    let words = list (pair str (pair long long)) in
    { fields = fields; matches = matches; total = total; total_found = total_found; time = time; words = words }
  in
  let get () =
    let st = long () in
    match catch Enum.to_enum<status>(st) with
    | None -> fail "run: unknown status code %d" st
    | Some Error | Some Retry -> `Err (str ())
    | Some Warning -> let warn = str () in `Ok (result (), Some warn)
    | Some Ok -> `Ok (result (), None)
  in
  List.map (fun _ -> get ()) reqs, w

(** Connect to searchd server and run given search query.
		Raises exception on failure.
    @return result set (see documentation for details).
*)
let query sock q ?index ?comment s =
  let s = build q ?index ?comment s in
  match run sock [s] with
  | [r], w -> r, w
  | _ -> fail "run"

(*
	def BuildExcerpts (self, docs, index, words, opts=None):
		"""
		Connect to searchd server and generate exceprts from given documents.
		"""
		if not opts:
			opts = {}
		if isinstance(words,unicode):
			words = words.encode('utf-8')

		assert(isinstance(docs, list))
		assert(isinstance(index, str))
		assert(isinstance(words, str))
		assert(isinstance(opts, dict))

		sock = self._Connect()

		if not sock:
			return None

		# fixup options
		opts.setdefault('before_match', '<b>')
		opts.setdefault('after_match', '</b>')
		opts.setdefault('chunk_separator', ' ... ')
		opts.setdefault('limit', 256)
		opts.setdefault('around', 5)

		# build request
		# v.1.0 req

		flags = 1 # (remove spaces)
		if opts.get('exact_phrase'):	flags |= 2
		if opts.get('single_passage'):	flags |= 4
		if opts.get('use_boundaries'):	flags |= 8
		if opts.get('weight_order'):	flags |= 16
		if opts.get('query_mode'):	flags |= 32
		if opts.get('force_all_words'):	flags |= 64
		
		# mode=0, flags
		req = [pack('>2L', 0, flags)]

		# req index
		req.append(pack('>L', len(index)))
		req.append(index)

		# req words
		req.append(pack('>L', len(words)))
		req.append(words)

		# options
		req.append(pack('>L', len(opts['before_match'])))
		req.append(opts['before_match'])

		req.append(pack('>L', len(opts['after_match'])))
		req.append(opts['after_match'])

		req.append(pack('>L', len(opts['chunk_separator'])))
		req.append(opts['chunk_separator'])

		req.append(pack('>L', int(opts['limit'])))
		req.append(pack('>L', int(opts['around'])))

		# documents
		req.append(pack('>L', len(docs)))
		for doc in docs:
			if isinstance(doc,unicode):
				doc = doc.encode('utf-8')
			assert(isinstance(doc, str))
			req.append(pack('>L', len(doc)))
			req.append(doc)

		req = ''.join(req)

		# send query, get response
		length = len(req)

		# add header
		req = pack('>2HL', SEARCHD_COMMAND_EXCERPT, VER_COMMAND_EXCERPT, length)+req
		wrote = sock.send(req)

		response = self._GetResponse(sock, VER_COMMAND_EXCERPT )
		if not response:
			return []

		# parse response
		pos = 0
		res = []
		rlen = len(response)

		for i in range(len(docs)):
			length = unpack('>L', response[pos:pos+4])[0]
			pos += 4

			if pos+length > rlen:
				self._error = 'incomplete reply'
				return []

			res.append(response[pos:pos+length])
			pos += length

		return res


	def UpdateAttributes ( self, index, attrs, values ):
		"""
		Update given attribute values on given documents in given indexes.
		Returns amount of updated documents (0 or more) on success, or -1 on failure.

		'attrs' must be a list of strings.
		'values' must be a dict with int key (document ID) and list of int values (new attribute values).

		Example:
			res = cl.UpdateAttributes ( 'test1', [ 'group_id', 'date_added' ], { 2:[123,1000000000], 4:[456,1234567890] } )
		"""
		assert ( isinstance ( index, str ) )
		assert ( isinstance ( attrs, list ) )
		assert ( isinstance ( values, dict ) )
		for attr in attrs:
			assert ( isinstance ( attr, str ) )
		for docid, entry in values.items():
			assert ( isinstance ( docid, int ) )
			assert ( isinstance ( entry, list ) )
			assert ( len(attrs)==len(entry) )
			for val in entry:
				assert ( isinstance ( val, int ) )

		# build request
		req = [ pack('>L',len(index)), index ]

		req.append ( pack('>L',len(attrs)) )
		for attr in attrs:
			req.append ( pack('>L',len(attr)) + attr )

		req.append ( pack('>L',len(values)) )
		for docid, entry in values.items():
			req.append ( pack('>Q',docid) )
			for val in entry:
				req.append ( pack('>L',val) )

		# connect, send query, get response
		sock = self._Connect()
		if not sock:
			return None

		req = ''.join(req)
		length = len(req)
		req = pack ( '>2HL', SEARCHD_COMMAND_UPDATE, VER_COMMAND_UPDATE, length ) + req
		wrote = sock.send ( req )

		response = self._GetResponse ( sock, VER_COMMAND_UPDATE )
		if not response:
			return -1

		# parse response
		updated = unpack ( '>L', response[0:4] )[0]
		return updated


	def BuildKeywords ( self, query, index, hits ):
		"""
		Connect to searchd server, and generate keywords list for a given query.
		Returns None on failure, or a list of keywords on success.
		"""
		assert ( isinstance ( query, str ) )
		assert ( isinstance ( index, str ) )
		assert ( isinstance ( hits, int ) )

		# build request
		req = [ pack ( '>L', len(query) ) + query ]
		req.append ( pack ( '>L', len(index) ) + index )
		req.append ( pack ( '>L', hits ) )

		# connect, send query, get response
		sock = self._Connect()
		if not sock:
			return None

		req = ''.join(req)
		length = len(req)
		req = pack ( '>2HL', SEARCHD_COMMAND_KEYWORDS, VER_COMMAND_KEYWORDS, length ) + req
		wrote = sock.send ( req )

		response = self._GetResponse ( sock, VER_COMMAND_KEYWORDS )
		if not response:
			return None

		# parse response
		res = []

		nwords = unpack ( '>L', response[0:4] )[0]
		p = 4
		max_ = len(response)

		while nwords>0 and p<max_:
			nwords -= 1

			length = unpack ( '>L', response[p:p+4] )[0]
			p += 4
			tokenized = response[p:p+length]
			p += length

			length = unpack ( '>L', response[p:p+4] )[0]
			p += 4
			normalized = response[p:p+length]
			p += length

			entry = { 'tokenized':tokenized, 'normalized':normalized }
			if hits:
				entry['docs'], entry['hits'] = unpack ( '>2L', response[p:p+8] )
				p += 8

			res.append ( entry )

		if nwords>0 or p>max_:
			self._error = 'incomplete reply'
			return None

		return res

	### persistent connections

	def Open(self):
		if self._socket:
			self._error = 'already connected'
			return None
		
		server = self._Connect()
		if not server:
			return None

		# command, command version = 0, body length = 4, body = 1
		request = pack ( '>hhII', SEARCHD_COMMAND_PERSIST, 0, 4, 1 )
		server.send ( request )
		
		self._socket = server
		return True

	def Close(self):
		if not self._socket:
			self._error = 'not connected'
			return
		self._socket.close()
		self._socket = None
	
	def EscapeString(self, string):
		return re.sub(r"([=\(\)|\-!@~\"&/\\\^\$\=])", r"\\\1", string)


	def FlushAttributes(self):
		sock = self._Connect()
		if not sock:
			return -1

		request = pack ( '>hhI', SEARCHD_COMMAND_FLUSHATTRS, VER_COMMAND_FLUSHATTRS, 0 ) # cmd, ver, bodylen
		sock.send ( request )

		response = self._GetResponse ( sock, VER_COMMAND_FLUSHATTRS )
		if not response or len(response)!=4:
			self._error = 'unexpected response length'
			return -1

		tag = unpack ( '>L', response[0:4] )[0]
		return tag
*)
