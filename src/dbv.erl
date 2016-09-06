-module(dbv).
-export([main/1, do/1, read_schema/1]).

-define(LOG(FMT, ARGS), io:format(FMT ++ "~n", ARGS)).

-record(doc, {id, key, value, doc}).

main(Args) ->
	case Args of
		[Host, Schema] ->
			read_schema(Schema),
			do(Host);
		_ ->
			io:format("Usage: dbv Host SchemaPath~n")
	end.

do(Host) ->
	S = couchbeam:server_connection(Host, []),
	{ok, Dbs} = couchbeam:all_dbs(S),
	[ handle_db(S, Db) || Db <- Dbs ].

handle_db(S, Name) ->
	?LOG("processing db:~p", [Name]),
	{ok, Db} = couchbeam:open_db(S, http_uri:encode(erlang:binary_to_list(Name)), []),
	{ok, Docs} = couchbeam_view:all(Db, [include_docs]),
	[ handle_doc(parse_doc(Doc)) || Doc <- Docs ],
	ok.

parse_doc({Doc}) ->
	#doc{
		id=proplists:get_value(<<"id">>, Doc),
		key=proplists:get_value(<<"key">>, Doc),
		value=proplists:get_value(<<"value">>, Doc),
		doc=proplists:get_value(<<"doc">>, Doc)
	}.
	
pvt_type({Doc}) ->
	proplists:get_value(<<"pvt_type">>, Doc).

handle_doc(#doc{doc=Doc}) ->
	validate(pvt_type(Doc), Doc).

id({Doc}) -> proplists:get_value(<<"_id">>, Doc).

validate(undefined, _) -> skip;
validate(Type, Doc) ->
	Plural = erlang:binary_to_list(<<Type/binary, "s">>),
	Re = jesse:validate(Plural, Doc),
	?LOG("validate type:~p id:~p re:~p", [Type, id(Doc), map_re(Re)]).

map_re({ok, _}) -> "ok";
map_re({error, {database_error, Schema, Err}}) -> io_lib:format("~p:~p", [Err, Schema]);
map_re({error, Errors}) ->
	ErrMap =
		fun
			({data_invalid, _, Type, Value, Path}) ->
				io_lib:format("type:~p value:~p", [Type, Path]);
			({schema_invalid, _, Error}) ->
				io_lib:format("schema:~p", [Error])
		end,
	lists:map(ErrMap, Errors).

read_schema(Path) ->
	{ok, Files} = file:list_dir(Path),
	[ add_file(filename:join(Path,File)) || File <- Files ].

add_file(File) ->
	{ok, Content} = file:read_file(File),
	Ext = filename:extension(File),
	Name = filename:basename(File, Ext),
	Schema = jiffy:decode(Content),
	jesse:add_schema(Name, Schema).
