%% This test suite verifies the plugin as a blackbox.
%% It assumes that the broker with the plugin installed
%% is running on endpoint defined in environment variable
%% EMQX_MQTT_ENDPOINT.
-module(emqx_sdv_fanout_integration_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    {apps, Apps} = lists:keyfind(apps, 1, Config),
    ok = emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

%% Vehicles connect and subscribe, then a batch is published by SDV platform.
t_realtime_dispatch(_Config) ->
    %% Test with a unique set of VINs to avoid contamination from previous tests.
    UniqueId = erlang:system_time(millisecond),
    VINs = [vin(UniqueId, I) || I <- lists:seq(1, 10)],
    {ok, SubPids} = start_vehicle_clients(VINs),
    try
        {ok, PubPid} = start_batch_publisher(),
        try
            {ok, {RequestId, Data}} = publish_batch(PubPid, VINs),
            ok = assert_payload_received(SubPids, RequestId, Data)
        after
            ok = stop_client(PubPid)
        end
    after
        ok = stop_clients(SubPids)
    end.

%% A batch is published by SDV platform, then vehicles connect and subscribe.
%% Expect the dispatches to be triggered by heartbeat messages.
t_late_subscribe(_Config) ->
    UniqueId = erlang:system_time(millisecond),
    VINs = [vin(UniqueId, I) || I <- lists:seq(1, 10)],
    {ok, PubPid} = start_batch_publisher(),
    try
        {ok, {RequestId, Data}} = publish_batch(PubPid, VINs),
        {ok, SubPids} = start_vehicle_clients(VINs),
        receive
            {publish_received, _, _, _} ->
                ct:fail("unexpected publish message received")
        after 1000 ->
            ok
        end,
        HeartbeatPid = send_heartbeats(SubPids),
        try
            ok = assert_payload_received(SubPids, RequestId, Data)
        after
            HeartbeatPid ! stop,
            ok = stop_clients(SubPids)
        end
    after
        ok = stop_client(PubPid)
    end.

%% Clients will not receive dispatch after reconnect (with session persisted)
%% but will receive after heartbeat is sent.
t_disconnected_session_does_not_receive_dispatch(_Config) ->
    UniqueId = erlang:system_time(millisecond),
    VINs = [vin(UniqueId, I) || I <- lists:seq(1, 5)],
    {ok, PubPid} = start_batch_publisher(),
    try
        {ok, {RequestId, Data}} = publish_batch(PubPid, VINs),
        Opts = [{clean_start, false}, {properties, #{'Session-Expiry-Interval' => 5}}],
        {ok, SubPids0} = start_vehicle_clients(VINs, Opts),
        ok = stop_clients(SubPids0),
        {ok, SubPids} = start_vehicle_clients(VINs, Opts),
        ok = assert_session_persisted(SubPids),
        receive
            {publish_received, _, _, _} ->
                ct:fail("unexpected publish message received")
        after 1000 ->
            ok
        end,
        HeartbeatPid = send_heartbeats(SubPids),
        try
            ok = assert_payload_received(SubPids, RequestId, Data)
        after
            HeartbeatPid ! stop,
            ok = stop_clients(SubPids)
        end
    after
        ok = stop_client(PubPid)
    end.

%% The vehicle client crashes after receiving the first message,
%% then reconnects and process the redelivered message normally.
t_reconnect_after_disconnect(_Config) ->
    UniqueId = erlang:system_time(millisecond),
    VIN = vin(UniqueId, 1),
    {ok, PubPid} = start_batch_publisher(),
    try
        {ok, {RequestId, Data}} = publish_batch(PubPid, [VIN]),
        Opts = [{clean_start, false}, {properties, #{'Session-Expiry-Interval' => 5}}],
        {ok, SubPid1, Mref1} = start_reconnect_vehicle_client(VIN, true, Opts),
        HeartbeatPid = send_heartbeats([{VIN, SubPid1}]),
        receive
            {'DOWN', Mref1, process, SubPid1, _Reason} ->
                ok
        after 5000 ->
            ct:fail(#{reason => "client is not down as expected"})
        end,
        HeartbeatPid ! stop,
        {ok, SubPid2, _Mref2} = start_reconnect_vehicle_client(VIN, false, Opts),
        %% Expect the message to be redelivered without any trigger,
        %% because the last session terminated before PUBACK is sent.
        receive
            {publish_received, SubPid2, VIN, Msg} ->
                ?assertEqual(Data, maps:get(payload, Msg))
        after 5000 ->
            ct:fail(#{reason => "client did not receive the message as expected"})
        end,
        ok = stop_client(SubPid2)
    after
        ok = stop_client(PubPid)
    end.

%% Reproduce the non-clean-session takeover race: a QoS-1 fanout message is
%% delivered to a vehicle that disconnects abruptly before sending PUBACK.
%% The session retains the inflight message; the next client connection
%% triggers a DUP=1 redelivery. The vehicle then publishes a heartbeat,
%% which used to make the plugin dispatch the same message a second time
%% because the plugin's inflight ETS was cleaned up on the old SubPid's
%% DOWN. The fix in heartbeat/1 consults emqx's cached session stats and
%% suppresses the redispatch. This test asserts that the second connection
%% receives exactly one PUBLISH on the subscription, not two.
t_resume_does_not_redispatch(_Config) ->
    VIN = vin(erlang:system_time(millisecond), 1),
    {ok, PubPid} = start_batch_publisher(),
    try
        Owner = self(),
        Opts1 = [
            {clean_start, false},
            {auto_ack, never},
            {properties, #{'Session-Expiry-Interval' => 30}}
        ],
        {ok, Sub1} = start_held_vehicle_client(VIN, Owner, Opts1),
        Data =
            try
                {ok, {_RequestId, D}} = publish_batch(PubPid, [VIN]),
                %% First delivery — captured but not PUBACKed.
                #{payload := P1, dup := false} = receive_publish(Sub1, VIN, 5000),
                ?assertEqual(D, P1),
                D
            after
                ok = abrupt_disconnect(Sub1)
            end,
        %% Give the broker a moment to register the disconnection and
        %% release the old channel pid; otherwise the reconnect can race.
        timer:sleep(200),
        Opts2 = [
            {clean_start, false},
            %% Hold the PUBACK on the redelivered (DUP=1) message: the race
            %% window only exists while M1 is still inflight in the session.
            %% Auto-acking would clear the session inflight before our
            %% heartbeat reaches the plugin and the race would not fire.
            {auto_ack, never},
            {properties, #{'Session-Expiry-Interval' => 30}}
        ],
        {ok, Sub2} = start_held_vehicle_client(VIN, Owner, Opts2),
        try
            ?assertEqual(1, session_present(Sub2)),
            %% Expect the DUP=1 redelivery first.
            #{payload := P2, dup := true, packet_id := PktId} =
                receive_publish(Sub2, VIN, 5000),
            ?assertEqual(Data, P2),
            %% Now publish a heartbeat. Before the fix, this triggers a
            %% second dispatch of the same fanout message.
            {ok, _} = emqtt:publish(
                Sub2, bin([<<"ecp/">>, VIN, <<"/heartbeat">>]), <<>>, 1
            ),
            assert_no_publish(VIN, 1500),
            %% Clean up: PUBACK the held redelivery so the session can
            %% drain naturally rather than relying on the expiry timer.
            ok = emqtt:puback(Sub2, PktId)
        after
            ok = stop_client(Sub2)
        end
    after
        ok = stop_client(PubPid)
    end.

t_non_json_payload_causes_disconnect(_Config) ->
    test_invalid_trigger_payload(<<"not json">>).

t_invalid_payload_causes_disconnect(_Config) ->
    test_invalid_trigger_payload(<<"{\"ids\": [\"vin-1\"]}">>).

test_invalid_trigger_payload(Payload) ->
    {ok, PubPid} = start_batch_publisher(),
    unlink(PubPid),
    TriggerTopic = <<"$SDV-FANOUT/trigger">>,
    QoS = 1,
    ?assertMatch({error, {disconnected, _, _}}, emqtt:publish(PubPid, TriggerTopic, Payload, QoS)).

assert_payload_received([], _RequestId, _Data) ->
    ok;
assert_payload_received(SubPids, RequestId, Data) ->
    Remain =
        receive
            {publish_received, SubPid, VIN, Msg} ->
                ?assertMatch(#{qos := 1}, Msg),
                ?assertEqual(Data, maps:get(payload, Msg)),
                Topic = maps:get(topic, Msg),
                [<<"agent">>, VIN0, <<"proxy">>, <<"request">>, RequestId] = words(Topic),
                ?assertEqual(VIN, VIN0),
                case lists:member({VIN, SubPid}, SubPids) of
                    true ->
                        lists:delete({VIN, SubPid}, SubPids);
                    false ->
                        ct:fail(#{
                            reason => unexpected_subscriber,
                            got => {VIN, SubPid},
                            expected => SubPids
                        })
                end
        after 10000 ->
            ct:fail(#{
                reason => timeout,
                remain => SubPids
            })
        end,
    assert_payload_received(Remain, RequestId, Data).

start_batch_publisher() ->
    {Host, Port} = mqtt_endpoint(),
    {ok, Pid} = emqtt:start_link([
        {host, Host}, {port, Port}, {clientid, <<"sdv-batch-publisher">>}, {proto_ver, v5}
    ]),
    {ok, _} = emqtt:connect(Pid),
    {ok, Pid}.

%% Returns {Host, Port}. Accepts EMQX_MQTT_ENDPOINT as either "host" or
%% "host:port"; defaults to {"127.0.0.1", 1883}.
mqtt_endpoint() ->
    case os:getenv("EMQX_MQTT_ENDPOINT") of
        false ->
            {"127.0.0.1", 1883};
        Endpoint ->
            case string:split(Endpoint, ":") of
                [Host, PortStr] ->
                    {Host, list_to_integer(PortStr)};
                [Host] ->
                    {Host, 1883}
            end
    end.

publish_batch(Pid, VINs) ->
    DataID = integer_to_binary(erlang:system_time(millisecond)),
    DataTopic = bin(["$SDV-FANOUT/data/", DataID]),
    TriggerTopic = <<"$SDV-FANOUT/trigger">>,
    QoS = 1,
    Data = crypto:strong_rand_bytes(1024),
    RequestId = integer_to_binary(erlang:system_time(millisecond)),
    Trigger = emqx_utils_json:encode(#{ids => VINs, request_id => RequestId, data_id => DataID}),
    {ok, _} = emqtt:publish(Pid, DataTopic, Data, QoS),
    {ok, _} = emqtt:publish(Pid, TriggerTopic, Trigger, QoS),
    {ok, {RequestId, Data}}.

%% Start a number of vehicle clients which subscribes to the topic
%% "agent/<vin>/proxy/request/+".
start_vehicle_clients(VINs) ->
    start_vehicle_clients(VINs, []).

start_vehicle_clients(VINs, Opts) ->
    Owner = self(),
    {Host, Port} = mqtt_endpoint(),
    lists:foldl(
        fun(VIN, {ok, Pids}) ->
            MsgHandler = #{publish => fun(Msg) -> Owner ! {publish_received, self(), VIN, Msg} end},
            {ok, Pid} = emqtt:start_link(
                [
                    {host, Host},
                    {port, Port},
                    {clientid, VIN},
                    {proto_ver, v5},
                    {msg_handler, MsgHandler}
                ] ++ Opts
            ),
            {ok, _} = emqtt:connect(Pid),
            QoS = 1,
            {ok, _, _} = emqtt:subscribe(Pid, sub_topic(VIN), QoS),
            {ok, [{VIN, Pid} | Pids]}
        end,
        {ok, []},
        VINs
    ).

start_reconnect_vehicle_client(VIN, ShouldDisconnect, Opts) ->
    Owner = self(),
    {Host, Port} = mqtt_endpoint(),
    MsgHandler = #{
        publish => fun(Msg) ->
            ct:pal("publish received from EMQX: ~p", [Msg]),
            case ShouldDisconnect of
                true ->
                    exit(self(), kill);
                false ->
                    Owner ! {publish_received, self(), VIN, Msg}
            end
        end
    },
    {ok, Pid} = emqtt:start_link(
        [
            {host, Host},
            {port, Port},
            {clientid, VIN},
            {proto_ver, v5},
            {msg_handler, MsgHandler}
        ] ++ Opts
    ),
    {ok, _} = emqtt:connect(Pid),
    Mref = monitor(process, Pid),
    unlink(Pid),
    QoS = 1,
    {ok, _, _} = emqtt:subscribe(Pid, sub_topic(VIN), QoS),
    {ok, Pid, Mref}.

sub_topic(VIN) ->
    bin(["agent/", VIN, "/proxy/request/+"]).

stop_clients(Pids) ->
    lists:foreach(
        fun({_, Pid}) ->
            stop_client(Pid)
        end,
        Pids
    ).

stop_client(Pid) ->
    unlink(Pid),
    ok = emqtt:stop(Pid).

vin(UniqueId, I) ->
    list_to_binary(["vin-", integer_to_list(UniqueId), "-", integer_to_list(I)]).

words(Topic) ->
    binary:split(Topic, <<"/">>, [global]).

%% First send online message, then send heartbeat messages.
send_heartbeats(SubPids) ->
    lists:foreach(
        fun({VIN, Pid}) ->
            Topic = bin([<<"ecp/">>, VIN, <<"/online">>]),
            {ok, _} = emqtt:publish(Pid, Topic, <<"">>, 1)
        end,
        SubPids
    ),
    spawn_link(fun() -> heartbeat_loop(SubPids) end).

heartbeat_loop(SubPids) ->
    receive
        stop ->
            exit(normal)
    after 1000 ->
        lists:foreach(
            fun({VIN, Pid}) ->
                Topic = bin([<<"ecp/">>, VIN, <<"/heartbeat">>]),
                {ok, _} = emqtt:publish(Pid, Topic, <<"">>, 1)
            end,
            SubPids
        ),
        heartbeat_loop(SubPids)
    end.

bin(IoData) ->
    iolist_to_binary(IoData).

assert_session_persisted(SubPids) ->
    lists:foreach(
        fun({_VIN, Pid}) ->
            Info = emqtt:info(Pid),
            InfoMap = maps:from_list(Info),
            ?assertEqual(1, maps:get(session_present, InfoMap))
        end,
        SubPids
    ).

%% Like start_vehicle_clients/2 but for a single VIN. Forwards every PUBLISH
%% to Owner as `{publish_received, SubPid, VIN, Msg}'. The caller is
%% responsible for any PUBACK; combine with `{auto_ack, never}' in Opts to
%% hold messages inflight in the broker session.
start_held_vehicle_client(VIN, Owner, Opts) ->
    {Host, Port} = mqtt_endpoint(),
    MsgHandler = #{
        publish => fun(Msg) -> Owner ! {publish_received, self(), VIN, Msg} end
    },
    {ok, Pid} = emqtt:start_link(
        [
            {host, Host},
            {port, Port},
            {clientid, VIN},
            {proto_ver, v5},
            {msg_handler, MsgHandler}
        ] ++ Opts
    ),
    {ok, _} = emqtt:connect(Pid),
    QoS = 1,
    {ok, _, _} = emqtt:subscribe(Pid, sub_topic(VIN), QoS),
    {ok, Pid}.

%% Wait for a single PUBLISH from VIN routed through the client process.
receive_publish(SubPid, VIN, TimeoutMs) ->
    receive
        {publish_received, SubPid, VIN, Msg} ->
            Msg
    after TimeoutMs ->
        ct:fail(#{
            reason => publish_not_received,
            sub_pid => SubPid,
            vin => VIN
        })
    end.

%% Assert no PUBLISH arrives for this VIN within the window.
assert_no_publish(VIN, TimeoutMs) ->
    receive
        {publish_received, _, VIN, Msg} ->
            ct:fail(#{
                reason => unexpected_duplicate_publish,
                vin => VIN,
                msg => Msg
            })
    after TimeoutMs ->
        ok
    end.

%% Tear down a client without sending DISCONNECT, so the broker treats it
%% as an abrupt network drop and the session is preserved for resume.
abrupt_disconnect(Pid) ->
    unlink(Pid),
    %% kill bypasses emqtt's graceful shutdown path; no MQTT DISCONNECT
    %% is sent before the socket closes.
    exit(Pid, kill),
    receive
        {'EXIT', Pid, _} -> ok
    after 0 -> ok
    end,
    ok.

session_present(Pid) ->
    Info = emqtt:info(Pid),
    InfoMap = maps:from_list(Info),
    maps:get(session_present, InfoMap).
