package rocketmqtest

import "testing"

func TestSendNormalMassage(t *testing.T) {
	type args struct {
		name, endpoint, topic, tag, key, body, accessKey, accessSecret string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test send normal massage",
			args: args{
				endpoint:     "",
				topic:        "",
				tag:          "",
				key:          "",
				body:         "",
				accessKey:    "",
				accessSecret: "",
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := NewBaseInfo(tt.args.endpoint, tt.args.topic, tt.args.tag, tt.args.key, tt.args.body, tt.args.accessKey, tt.args.accessSecret)
			if err := info.SendNormalMessage(); err != nil {
				t.Errorf("failed to send normal message, err:%s", err)
			}
			//todo 添加验证
		})
	}
}
