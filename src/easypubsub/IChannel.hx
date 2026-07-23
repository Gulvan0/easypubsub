package easypubsub;

@:autoBuild(easypubsub.macros.ChannelMacro.build())
interface IChannel
{
	public function toJson():Dynamic;
	public function hash():String;
}
