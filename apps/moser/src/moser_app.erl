-module(moser_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    error_logger:info_msg("Starting moser~p ~p~n", [_StartType, _StartArgs]),
    moser_sup:start_link().

stop(_State) ->
    ok.
