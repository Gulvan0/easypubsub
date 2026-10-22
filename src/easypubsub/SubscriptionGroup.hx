package easypubsub;

import jsonmodel.JsonUnserializable;
import morestd.Never;

/**
	Subscriptions to several channels of the same type, sharing their handlers and detached together.
	Every handler receives the channel the event came from.
**/
class SubscriptionGroup<TChannel:IChannel>
{
	private final engine:IPubSubEngine;
	private final subscriptions:Array<Subscription<TChannel>>;

	public function new(engine:IPubSubEngine, subscriptions:Array<Subscription<TChannel>>)
	{
		this.engine = engine;
		this.subscriptions = subscriptions;
	}

	// see Subscription.onEvent for TEventChannel
	@:generic
	public function onEvent<TPayload:JsonUnserializable, TEventChannel:TChannel>(kind:Class<IEvent<TPayload, TEventChannel>>, handler:TPayload->TChannel->Void):SubscriptionGroup<TChannel>
	{
		for (subscription in subscriptions)
			subscription.onEvent(kind, (payload, source) -> handler(payload, source.channel));

		return this;
	}

	public function onAtomicEvent(kind:Class<IEvent<Never, TChannel>>, handler:TChannel->Void):SubscriptionGroup<TChannel>
	{
		for (subscription in subscriptions)
			subscription.onAtomicEvent(kind, source -> handler(source.channel));

		return this;
	}

	/** Called with the channel the error concerns, along with the error kind and details **/
	public function onError(handler:TChannel->String->Dynamic->Void):SubscriptionGroup<TChannel>
	{
		for (subscription in subscriptions)
			subscription.onError((errorKind, details) -> handler(subscription.channel, errorKind, details));

		return this;
	}

	/** Detaches every subscription in a single message. The group is empty afterwards **/
	public function detach()
	{
		engine.unsubMany(subscriptions);
		subscriptions.resize(0);
	}
}
