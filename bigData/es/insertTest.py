from elasticsearch import Elasticsearch

body = {
	'request': {
		'message': {
			'head': {
				'rcmdStoreName': '乐贷款-虚拟网点',
				'transReqTime': 20180731092613,
				'productCode': '017313',
				'netCode': 'N02070100000011',
				'transType': 90,
				'merchantId': 'N02070100000000',
				'rcmdStoreCode': 'N02070100000011',
				'transSeqNo': '20180731092613230361825567535700',
				'customerId': '20180205153900572872',
				'transCode': 6003,
				'operatorCode': 17717602280,
				'version': 1
			}
		}
	},
	'response': {
		'message': {
			'head': {
				'transRepTime': 20180731092657,
				'netCode': 'N02070100000011',
				'transSeqNo': '20180731092613230361825567535700',
				'operatorCode': 17717602280,
				'version': 1,
				'transRepSeqNo': '20180731092657192000000008114874',
				'errorMsg': '成功',
				'returnCode': '000000',
				'transReqTime': 20180731092613,
				'productCode': '017313',
				'transType': 90,
				'merchantId': 'N02070100000000',
				'customerId': '20180205153900572872',
				'transCode': 6003
			},
			'Signature': {
				'xmlns': 'http://www.w3.org/2000/09/xmldsig#',
				'SignatureValue': 'aWbJMTpXLQeE0ZGKPSdMdANC8N30u4OUpTB9nBQauT8iwXrBXfCKyPp04iSX6cTvV1F/c3aP08D3NLPpkwqlX0H83UTJdcSxuUWUOw49xd9nPkihyWHHd9yKXwbpmCxz/gmVPCbGiKT0Gh0xOBxVyD0Idf3wRhlKmmfedR4hDAo=',
				'SignedInfo': {
					'Reference': {
						'Transforms': {
							'Transform': {
								'Algorithm': 'http://www.w3.org/2000/09/xmldsig#enveloped-signature'
							}
						},
						'DigestMethod': {
							'Algorithm': 'http://www.w3.org/2000/09/xmldsig#sha1'
						},
						'DigestValue': 'mR7dODi2MbTxmc700lLfpvdtD4w=',
						'URI': ''
					},
					'CanonicalizationMethod': {
						'Algorithm': 'http://www.w3.org/TR/2001/REC-xml-c14n-20010315'
					},
					'SignatureMethod': {
						'Algorithm': 'http://www.w3.org/2000/09/xmldsig#rsa-sha1'
					}
				}
			},
			'body': {
				'paperList': {
					'paper': {
						'questionList': {
							'question': [{
								'questionId': 1,
								'choiceList': {
									'choice': [{
										'choiceId': 'A',
										'choiceValue': '2009.04'
									}, {
										'choiceId': 'B',
										'choiceValue': '2008.11'
									}, {
										'choiceId': 'C',
										'choiceValue': '2009.09'
									}, {
										'choiceId': 'D',
										'choiceValue': '2010.02'
									}, {
										'choiceId': 'E',
										'choiceValue': '以上皆否'
									}]
								},
								'type': 1,
								'content': '请问您首张信用卡发卡年月？'
							}, {
								'questionId': 2,
								'choiceList': {
									'choice': [{
										'choiceId': 'A',
										'choiceValue': '厦门大学'
									}, {
										'choiceId': 'B',
										'choiceValue': '清华北大2'
									}, {
										'choiceId': 'C',
										'choiceValue': '上海工程技术大学'
									}, {
										'choiceId': 'D',
										'choiceValue': '清华北大'
									}, {
										'choiceId': 'E',
										'choiceValue': '以上皆否'
									}]
								},
								'type': 1,
								'content': '请问您毕业学校为？'
							}, {
								'questionId': 3,
								'choiceList': {
									'choice': [{
										'choiceId': 'A',
										'choiceValue': '18000'
									}, {
										'choiceId': 'B',
										'choiceValue': '17500'
									}, {
										'choiceId': 'C',
										'choiceValue': '17000'
									}, {
										'choiceId': 'D',
										'choiceValue': '18500'
									}, {
										'choiceId': 'E',
										'choiceValue': '以上皆否'
									}]
								},
								'type': 1,
								'content': '请问您信用卡近6个月平均使用额度？'
							}, {
								'questionId': 4,
								'choiceList': {
									'choice': [{
										'choiceId': 'A',
										'choiceValue': '雷尔柳'
									}, {
										'choiceId': 'B',
										'choiceValue': '杨超南'
									}, {
										'choiceId': 'C',
										'choiceValue': '范馨'
									}, {
										'choiceId': 'D',
										'choiceValue': '支凝安'
									}, {
										'choiceId': 'E',
										'choiceValue': '以上皆否'
									}]
								},
								'type': 1,
								'content': '请问您的配偶为？'
							}, {
								'questionId': 5,
								'choiceList': {
									'choice': [{
										'choiceId': 'A',
										'choiceValue': 'dddd5'
									}, {
										'choiceId': 'B',
										'choiceValue': 'dddd1'
									}, {
										'choiceId': 'C',
										'choiceValue': 'dddd4'
									}, {
										'choiceId': 'D',
										'choiceValue': 'dddd3'
									}, {
										'choiceId': 'E',
										'choiceValue': '以上皆否'
									}]
								},
								'type': 1,
								'content': '请问您的籍贯为？'
							}]
						},
						'paperId': '341980C9C5714E8987E9D4F80AD30A43'
					}
				},
				'surveyCode': '1D47DFF3D55E4777B550E5B84121D979'
			}
		}
    }
}

es = Elasticsearch(['localhost:9200'])
ret = es.index(index = "index",doc_type = 'test',body=body)
