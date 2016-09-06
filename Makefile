devel:
	rm -rf _rel
	bin/rebar get-deps
	bin/rebar co
	bin/relx -d

master:
	rm -rf _rel
	bin/rebar get-deps
	bin/rebar co
	bin/relx

test:
	cd t && ct_run -pa ../apps/*/ebin -dir ../apps/web/t/

script:
	bin/rebar escriptize

console: devel
	_rel/dbv/bin/dbv console
