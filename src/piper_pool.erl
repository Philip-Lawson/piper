-module(piper_pool).

-behaviour(gen_server).

%% API functions
-export([start_link/3,
         process_data/2,
         finish/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {workers,
                next_link}).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(WorkerFun, NextLink, NumWorkers) ->
    Args = [WorkerFun, NextLink, NumWorkers],
    PoolName = get_pool_name(WorkerFun),
    gen_server:start_link({local, PoolName}, ?MODULE, Args, []).

process_data(WorkerPool, Data) ->
    gen_server:cast(WorkerPool, {process_data, Data}).

finish(WorkerPool) ->
    gen_server:cast(WorkerPool, stop).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([WorkerFun, NextLink, NumWorkers]) ->
    WorkerArgs = [{worker_fun, WorkerFun}, 
                  {next_link, NextLink}],
    Workers = start_workers(WorkerArgs, NumWorkers),
    {ok, #state{workers = Workers, next_link = NextLink}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({process_data, Data}, #state{workers = Workers} = State) ->
    {{value, Worker}, Queue} = queue:out(Workers),
    piper_worker:do_work(Worker, Data),
    {noreply, State#state{workers = queue:in(Worker, Queue)}};
handle_cast(stop, #state{next_link = NextLink,
                         workers = Workers} = State) ->
    stop_workers(Workers),
    gen_server:cast(NextLink, stop),
    {stop, normal, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

start_workers(WorkerArgs, NumWorkers) ->
    do_start_workers(WorkerArgs, queue:new(), NumWorkers).

do_start_workers(_WorkerArgs, Workers, 0) ->
    Workers;
do_start_workers(WorkerArgs, Workers, N) ->
    WorkerName = get_worker_name(WorkerArgs, N),
    {ok, Pid} = piper_worker:start_link(WorkerName, WorkerArgs),
    do_start_workers(WorkerArgs, queue:in(Pid, Workers), N-1).

get_pool_name(WorkerFun) ->
    PoolString = erlang:fun_to_list(WorkerFun) ++ "_pool",
    erlang:list_to_atom(PoolString).

get_worker_name(WorkerArgs, N) ->
    Fun = proplists:get_value(worker_fun, WorkerArgs),
    NameString = erlang:fun_to_list(Fun) ++ erlang:integer_to_list(N),
    erlang:list_to_atom(NameString).

stop_workers(WorkerQueue) ->
    Workers = queue:to_list(WorkerQueue),
    lists:map(fun(Worker) -> piper_worker:stop(Worker) end,
              Workers).
