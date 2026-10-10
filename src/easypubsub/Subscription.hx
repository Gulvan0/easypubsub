package easypubsub;

import morestd.Never;
import morestd.StringSetMap;
import jsonmodel.JsonUnserializable;

using morestd.extensions.StringExtension;

class Subscription<TChannel:IChannel>
{
	private final engine:IPubSubEngine;

	public final channel:TChannel;

	private final handlers:StringSetMap<Dynamic->Void> = new StringSetMap();
	private final anyEventHandlers:Array<String->Dynamic->Void> = [];

	public function new(engine:IPubSubEngine, channel:TChannel)
	{
		this.engine = engine;
		this.channel = channel;
	}

	private function getEventKind<T, TEventChannel:IChannel>(kind:Class<IEvent<T, TEventChannel>>):String
	{
		return Type.getClassName(kind).lastDelimitedPart(".").toSnakeCase();
	}

	// TEventChannel only keys @:generic's expansion by channel too: keyed by the payload alone, the first channel to use a payload type would be baked into it
	@:generic
	public function onEvent<TPayload:JsonUnserializable, TEventChannel:TChannel>(kind:Class<IEvent<TPayload, TEventChannel>>, handler:TPayload->Subscription<TChannel>->Void):Subscription<TChannel>
	{
		function callback(rawPayload:Dynamic)
		{
			handler(new TPayload(RawJson(rawPayload)), this);
		}

		handlers.add(getEventKind(kind), callback);

		return this;
	}

	public function onAtomicEvent(kind:Class<IEvent<Never, TChannel>>, handler:Subscription<TChannel>->Void):Subscription<TChannel>
	{
		function callback(_:Dynamic)
		{
			handler(this);
		}

		handlers.add(getEventKind(kind), callback);

		return this;
	}

	// see onEvent for TEventChannel
	@:generic
	public function onEventLight<TPayload:JsonUnserializable, TEventChannel:TChannel>(kind:Class<IEvent<TPayload, TEventChannel>>, handler:TPayload->Void):Subscription<TChannel>
	{
		function callback(rawPayload:Dynamic)
		{
			handler(new TPayload(RawJson(rawPayload)));
		}

		handlers.add(getEventKind(kind), callback);

		return this;
	}

	public function onAtomicEventLight(kind:Class<IEvent<Never, TChannel>>, handler:Void->Void):Subscription<TChannel>
	{
		function callback(_:Dynamic)
		{
			handler();
		}

		handlers.add(getEventKind(kind), callback);

		return this;
	}

	public function onAnyEvent(handler:String->Dynamic->Void):Subscription<TChannel>
	{
		anyEventHandlers.push(handler);
		return this;
	}

	public function dropAllHandlers<T>(kind:Null<Class<IEvent<T, TChannel>>> = null)
	{
		if (kind == null) {
			handlers.clear();
			anyEventHandlers.resize(0);
		} else {
			handlers.removeAll(getEventKind(kind));
		}
	}

	@:allow(easypubsub.PubSubEngine)
	private function dispatch(eventKind:String, rawPayload:Dynamic)
	{
		for (handler in handlers.get(eventKind))
			handler(rawPayload);
		for (handler in anyEventHandlers)
			handler(eventKind, rawPayload);
	}

	public function branch():Subscription<TChannel>
	{
		return engine.sub(channel);
	}

	public function detach()
	{
		engine.unsub(this);
	}
}
