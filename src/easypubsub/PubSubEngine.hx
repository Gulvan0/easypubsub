package easypubsub;

import morestd.Json as JsonUtils;
import morestd.StringSetMap;
import morestd.BackoffDelayTimer;
import hx.ws.Types.MessageType;
import js.html.Event;
import morestd.DateTime;
import morestd.RefreshableTimer;
import haxe.Json;
import haxe.Timer;
import hx.ws.WebSocket;

class PubSubEngine implements IPubSubEngine
{
	private var websocket:WebSocket;
	private var pingTimer:RefreshableTimer;
	private var reconnectTimer:BackoffDelayTimer;
	private var sessionPending:Bool = false;

	private var lastServerMessageUnixSecs:Int;

	public var activeSubscriptions:StringSetMap<Subscription<IChannel>> = new StringSetMap();
	public var connected(default, null):Bool = false;
	public var hasEverBeenConnected(default, null):Bool = false;

	/** Called when the server rejects the token sent with the session **/
	public var onAuthError:Null<Void->Void> = null;

	/**
		Tells whether an event kind (snake_case) is a channel's snapshot, i.e. the event replacing the whole state.
		Together with `expectsSnapshot`, makes subscriptions ignore the events arriving before their snapshot:
		the server registers a subscriber before computing the snapshot, so such events are already reflected in it.
		Gating is off while this is null.
	**/
	public var snapshotEventKind:Null<String->Bool> = null;

	/** Whether the channel is followed by a snapshot upon subscription. Channels without one are never gated **/
	public var expectsSnapshot:Null<IChannel->Bool> = null;

	private var tokenRetriever:Null<Void->Null<String>> = null;
	private var lastActivityUnixSecsRetriever:Null<Void->Null<Int>> = null;
	private var onInitialConnectionFailed:Null<Void->Void> = null;

	private var token(get, never):Null<String>;
	private var lastActivityUnixSecs(get, never):Null<Int>;

	public function new(url:String, ?onInitialConnectionFailed:Null<Void->Void> = null, ?tokenRetriever:Null<Void->Null<String>> = null,
			?lastActivityUnixSecsRetriever:Null<Void->Null<Int>> = null)
	{
		this.websocket = new WebSocket(url, false);
		this.pingTimer = new RefreshableTimer(30_000, heartbeatTick);
		this.reconnectTimer = new BackoffDelayTimer(1000, 1.5, 2, 16_000, -1000, 1000, connect.bind(null));

		this.onInitialConnectionFailed = onInitialConnectionFailed;
		this.tokenRetriever = tokenRetriever;
		this.lastActivityUnixSecsRetriever = lastActivityUnixSecsRetriever;
	}

	public function connect(?onInitialConnectionFailed:Null<Void->Void> = null)
	{
		if (onInitialConnectionFailed != null)
			this.onInitialConnectionFailed = onInitialConnectionFailed;

		websocket.onclose = onClose;
		websocket.onerror = onError;
		websocket.onmessage = onMessage;
		websocket.onopen = onOpen;

		lastServerMessageUnixSecs = DateTime.nowUnixSecs();

		websocket.open();
	}

	public function sendEvent(eventSlug:String, body:Null<{}> = null)
	{
		var message:{} = {event: eventSlug};
		if (body != null)
			Reflect.setField(message, "body", body);
		websocket.send(Json.stringify(message));
	}

	public function sendSerializedEvent(eventSlug:String, serializedBody:String)
	{
		websocket.send('{"event":"$eventSlug","body":$serializedBody}');
	}

	/** Tells the server who this connection acts as and makes its subscriptions exactly the active ones **/
	private function sendSession()
	{
		// the server answers the session with fresh snapshots
		for (channelHash in activeSubscriptions.keys())
			for (subscription in activeSubscriptions.get(channelHash))
				subscription.snapshotReceived = false;

		var canonicalChannelJsons:Array<String> = [for (canonicalChannelJson in activeSubscriptions.keys()) canonicalChannelJson];
		var body:String = '{"channels":[${canonicalChannelJsons.join(",")}]';
		if (token != null)
			body += ',"token":${Json.stringify(token)}';
		body += "}";
		sendSerializedEvent("session", body);
	}

	/**
		Re-sends the session, e.g. after the token has changed. Takes effect on connect otherwise.
		Deferred to the end of the current tick, so that the subscriptions changed meanwhile are sent along with it
		(and under the new identity) instead of one by one.
	**/
	public function reauth()
	{
		if (!connected || sessionPending)
			return;

		sessionPending = true;
		Timer.delay(flushSession, 0);
	}

	private function flushSession()
	{
		sessionPending = false;
		if (connected)
			sendSession();
	}

	private function sendSubOrUnsubEvent(channel:IChannel, sub:Bool)
	{
		var eventKind:String = sub ? "sub" : "unsub";
		sendEvent(eventKind, {channel: channel.toJson()});
	}

	public function sub<T:IChannel>(channel:T):Subscription<T>
	{
		var subscription:Subscription<T> = new Subscription(this, channel);
		activeSubscriptions.add(channel.hash(), cast subscription);
		// while disconnected, onOpen subscribes to every active channel once the connection is up
		if (connected && !sessionPending)
			sendSubOrUnsubEvent(channel, true);
		return subscription;
	}

	public function unsub<T:IChannel>(subscription:Subscription<T>)
	{
		var channelHash:String = subscription.channel.hash();
		activeSubscriptions.remove(channelHash, cast subscription);
		if (connected && !sessionPending && !activeSubscriptions.hasValues(channelHash))
			sendSubOrUnsubEvent(subscription.channel, false);
	}

	/** Same as `sub` for each channel, in a single message **/
	public function subMany<T:IChannel>(channels:Array<T>):Array<Subscription<T>>
	{
		var subscriptions:Array<Subscription<T>> = [];
		for (channel in channels)
		{
			var subscription:Subscription<T> = new Subscription(this, channel);
			activeSubscriptions.add(channel.hash(), cast subscription);
			subscriptions.push(subscription);
		}
		if (connected && !sessionPending && channels.length > 0)
			sendEvent("sub_many", {channels: channels.map(channel -> channel.toJson())});
		return subscriptions;
	}

	/** Same as `unsub` for each subscription, in a single message **/
	public function unsubMany<T:IChannel>(subscriptions:Array<Subscription<T>>)
	{
		var releasedHashes:Array<String> = [];
		var releasedChannels:Array<Dynamic> = [];
		for (subscription in subscriptions)
		{
			var channelHash:String = subscription.channel.hash();
			activeSubscriptions.remove(channelHash, cast subscription);
			if (!activeSubscriptions.hasValues(channelHash) && !releasedHashes.contains(channelHash))
			{
				releasedHashes.push(channelHash);
				releasedChannels.push(subscription.channel.toJson());
			}
		}
		if (connected && !sessionPending && releasedChannels.length > 0)
			sendEvent("unsub_many", {channels: releasedChannels});
	}

	/** `subMany`, with the subscriptions wrapped into a group sharing the handlers **/
	public function subGroup<T:IChannel>(channels:Array<T>):SubscriptionGroup<T>
	{
		return new SubscriptionGroup(this, subMany(channels));
	}

	/** Sends the heartbeat right away (if connected), e.g. to report fresh user activity sooner than the next scheduled one **/
	public function sendHeartbeat()
	{
		if (connected)
			sendEvent("ping", {last_activity: lastActivityUnixSecs});
	}

	private function heartbeatTick()
	{
		sendEvent("ping", {last_activity: lastActivityUnixSecs});

		if (DateTime.nowUnixSecs() - lastServerMessageUnixSecs > 60)
			websocket.close();
	}

	private function onOpen()
	{
		connected = true;
		hasEverBeenConnected = true;

		reconnectTimer.reset();
		pingTimer.start();

		sendSession();
	}

	private function onMessage(msg:MessageType)
	{
		var strContent:String = switch msg
		{
			case BytesMessage(content):
				content.readAllAvailableBytes().toString();
			case StrMessage(content):
				content;
		};

		lastServerMessageUnixSecs = DateTime.nowUnixSecs();

		try
		{
			final rawData:Dynamic = Json.parse(strContent);
			if (Reflect.field(rawData, "error") == "authorization_error")
			{
				// the server has dropped the identity and the subscriptions; the owner decides what to do with the token
				if (onAuthError != null)
					onAuthError();
				return;
			}
			final channelJson:Dynamic = Reflect.field(rawData, "channel");
			final channelHash:String = JsonUtils.canonize(channelJson);
			final errorKind:Null<String> = Reflect.field(rawData, "error");
			if (errorKind != null)
			{
				// errors concerning no particular channel have nobody to be routed to
				if (channelJson != null)
					for (subscription in activeSubscriptions.get(channelHash))
						subscription.dispatchError(errorKind, Reflect.field(rawData, "details"));
				return;
			}
			final eventKind:String = Reflect.field(rawData, "event");
			final rawPayload:Dynamic = Reflect.field(rawData, "body");
			for (subscription in activeSubscriptions.get(channelHash))
				subscription.dispatch(eventKind, rawPayload);
		}
		catch (e)
		{
			return;
		}
	}

	private function onClose()
	{
		connected = false;
		websocket.close();  // Just in case

		pingTimer.stop();
		reconnectTimer.startDelay();

		if (!hasEverBeenConnected && onInitialConnectionFailed != null)
			onInitialConnectionFailed();
	}

	private function onError(error:Event)
	{
		trace("Connection error: " + error.type);
	}

	private function get_token():Null<String>
	{
		return tokenRetriever != null ? tokenRetriever() : null;
	}

	private function get_lastActivityUnixSecs():Null<Int>
	{
		return lastActivityUnixSecsRetriever != null ? lastActivityUnixSecsRetriever() : null;
	}
}
