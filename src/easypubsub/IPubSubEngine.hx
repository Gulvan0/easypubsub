package easypubsub;

interface IPubSubEngine
{
	public function sub<T:IChannel>(channel:T):Subscription<T>;
	public function unsub<T:IChannel>(subscription:Subscription<T>):Void;
	public function subMany<T:IChannel>(channels:Array<T>):Array<Subscription<T>>;
	public function unsubMany<T:IChannel>(subscriptions:Array<Subscription<T>>):Void;
}
