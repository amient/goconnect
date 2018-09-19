package network

//
//func NetSplit() goc.MapFn {
//	return &netSplit{
//
//	}
//}
//
//type netSplit struct {
//	send goc.Elements
//	recv Receiver
//}
//
//func (n *netSplit) InType() reflect.Type {
//	return goc.ByteArrayType
//}
//
//func (n *netSplit) OutType() reflect.Type {
//	return goc.ByteArrayType
//}
//
//func (n *netSplit) Process(input *goc.Element) *goc.Element {
//	return &goc.Element{Value: string(input.Value.([]byte))}
//}
//
//func (n *netSplit) Close() {
//	close(n.send)
//	n.recv.Close()
//}
