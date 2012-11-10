DEPS = $(CURDIR)/deps

DIALYZER_OPTS = -Wunderspecs

# List dependencies that should be included in a cached dialyzer PLT file.
# DIALYZER_DEPS = deps/app1/ebin \
#                 deps/app2/ebin

DEPS_PLT = moser.plt

ERLANG_DIALYZER_APPS = asn1 \
                       compiler \
                       crypto \
                       edoc \
                       edoc \
                       erts \
                       eunit \
                       eunit \
                       gs \
                       hipe \
                       inets \
                       kernel \
                       mnesia \
                       mnesia \
                       observer \
                       public_key \
                       runtime_tools \
                       runtime_tools \
                       ssl \
                       stdlib \
                       syntax_tools \
                       syntax_tools \
                       tools \
                       webtool \
                       xmerl

use_locked_config = $(wildcard USE_REBAR_LOCKED)
ifeq ($(use_locked_config),USE_REBAR_LOCKED)
  rebar_config = rebar.config.lock
else
  rebar_config = rebar.config
endif
REBAR = rebar -C $(rebar_config)

all: compile eunit dialyzer

# Clean ebin and .eunit of this project
clean:
	@rebar clean skip_deps=true

# Clean this project and all deps
allclean:
	@rebar clean

compile: $(DEPS)
	@rebar compile

$(DEPS):
	@rebar get-deps

# Full clean and removal of all deps. Remove deps first to avoid
# wasted effort of cleaning deps before nuking them.
distclean:
	@rm -rf deps $(DEPS_PLT)
	@rebar clean

eunit:
	@rebar skip_deps=true eunit

test: eunit

$(DEPS_PLT):
	dialyzer -nn --output_plt $(DEPS_PLT) --build_plt --apps $(ERLANG_APPS)

plt_deps:
	dialyzer --plt $(DEPS_PLT) --output_plt $(DEPS_PLT) --add_to_plt deps/*/ebin


dialyze: compile $(DEPS_PLT)
	dialyzer -nn --plt $(DEPS_PLT) -Wunmatched_returns -Werror_handling -Wrace_conditions -r apps/pushy/ebin -I deps

rel: compile rel/moser
rel/moser:
	@cd rel;$(REBAR) generate 

relclean:
	@rm -rf rel/moser

devrel: rel
	@/bin/echo -n Symlinking deps and apps into release
	@$(foreach lib,$(wildcard apps/* deps/*), /bin/echo -n .;rm -rf rel/moser/lib/$(shell basename $(lib))-* \
           && ln -sf $(abspath $(lib)) rel/moser/lib;)
	@/bin/echo done.
	@/bin/echo  Run \'make update\' to pick up changes in a running VM.

update: compile
	@cd rel/moser;bin/moser restart

update_app: compile_app
	@cd rel/moser;bin/moser restart

doc:
	@rebar doc skip_deps=true

.PHONY: all compile eunit test dialyzer clean allclean distclean doc
