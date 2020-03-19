package utils

import (
	"bytes"
	"time"
	"net"
	"fmt"

	"github.com/cloudflare/goflow/v3/decoders/netflowlegacy"
	flowmessage "github.com/cloudflare/goflow/v3/pb"
	"github.com/cloudflare/goflow/v3/producer"
	"github.com/prometheus/client_golang/prometheus"
)

type StateNFLegacy struct {
	Transport Transport
	Logger    Logger

	MetricFlowStats				bool
	MetricFlowAggreateProtos 	[]string
	MetricFlowAggreatePorts		[]string
}

func (s *StateNFLegacy) FlowTrafficMetrics(fmsg *flowmessage.FlowMessage) {
	if s.MetricFlowStats {
		var ip_version = "6"
		if (net.IP(fmsg.SrcAddr).To4() != nil) {
			ip_version = "4"
		}
		var protocol = "undefined"
		for _, b := range s.MetricFlowAggreateProtos {
			if (b == fmt.Sprint(fmsg.Proto)) {
				protocol = fmt.Sprint(fmsg.Proto)
			}
		}
		var port = "undefined"
		for _, b := range s.MetricFlowAggreatePorts {
			if (b == fmt.Sprint(fmsg.SrcPort)) {
				port = fmt.Sprint(fmsg.SrcPort)
			} else if (b == fmt.Sprint(fmsg.DstPort)) {
				port = fmt.Sprint(fmsg.DstPort)
			}
		}

		MetricFlowStatsBytes.With(
			prometheus.Labels{
				"ip_version":  ip_version,
				"protocol": protocol,
				"port": port,
			}).
			Add(float64(fmsg.Bytes))

		MetricFlowStatsPackets.With(
			prometheus.Labels{
				"ip_version":  ip_version,
				"protocol": protocol,
				"port": port,
			}).
			Add(float64(fmsg.Packets))

		MetricFlowStatsFlows.With(
			prometheus.Labels{
				"ip_version":  ip_version,
				"protocol": protocol,
				"port": port,
			}).
			Inc()
	}
}

func (s *StateNFLegacy) DecodeFlow(msg interface{}) error {
	pkt := msg.(BaseMessage)
	buf := bytes.NewBuffer(pkt.Payload)
	key := pkt.Src.String()
	samplerAddress := pkt.Src
	if samplerAddress.To4() != nil {
		samplerAddress = samplerAddress.To4()
	}

	ts := uint64(time.Now().UTC().Unix())
	if pkt.SetTime {
		ts = uint64(pkt.RecvTime.UTC().Unix())
	}

	timeTrackStart := time.Now()
	msgDec, err := netflowlegacy.DecodeMessage(buf)

	if err != nil {
		switch err.(type) {
		case *netflowlegacy.ErrorVersion:
			NetFlowErrors.With(
				prometheus.Labels{
					"router": key,
					"error":  "error_version",
				}).
				Inc()
		}
		return err
	}

	switch msgDecConv := msgDec.(type) {
	case netflowlegacy.PacketNetFlowV5:
		NetFlowStats.With(
			prometheus.Labels{
				"router":  key,
				"version": "5",
			}).
			Inc()
		NetFlowSetStatsSum.With(
			prometheus.Labels{
				"router":  key,
				"version": "5",
				"type":    "DataFlowSet",
			}).
			Add(float64(msgDecConv.Count))
	}

	var flowMessageSet []*flowmessage.FlowMessage
	flowMessageSet, err = producer.ProcessMessageNetFlowLegacy(msgDec)

	timeTrackStop := time.Now()
	DecoderTime.With(
		prometheus.Labels{
			"name": "NetFlowV5",
		}).
		Observe(float64((timeTrackStop.Sub(timeTrackStart)).Nanoseconds()) / 1000)

	for _, fmsg := range flowMessageSet {
		fmsg.TimeReceived = ts
		fmsg.SamplerAddress = samplerAddress
		s.FlowTrafficMetrics(fmsg)
	}

	if s.Transport != nil {
		s.Transport.Publish(flowMessageSet)
	}

	return nil
}

func (s *StateNFLegacy) FlowRoutine(workers int, addr string, port int, reuseport bool) error {
	return UDPRoutine("NetFlowV5", s.DecodeFlow, workers, addr, port, reuseport, s.Logger)
}
