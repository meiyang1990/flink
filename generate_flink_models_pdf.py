#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Flink Models 模块源码分析 - PDF 生成脚本"""

import os, math
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm, cm
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, HRFlowable)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.graphics.shapes import Drawing, Line, Rect, String, Polygon

# === 字体注册 ===
FONT_CN = 'Helvetica'
FONT_CN_B = 'Helvetica-Bold'
for fp in ['/System/Library/Fonts/PingFang.ttc', '/System/Library/Fonts/STHeiti Light.ttc',
           '/System/Library/Fonts/Hiragino Sans GB.ttc', '/Library/Fonts/Arial Unicode.ttf']:
    if os.path.exists(fp):
        try:
            pdfmetrics.registerFont(TTFont('CN', fp, subfontIndex=0) if fp.endswith('.ttc') else TTFont('CN', fp))
            FONT_CN = FONT_CN_B = 'CN'
            break
        except: continue

def S(): return getSampleStyleSheet()

def mkstyles():
    s = S()
    def a(n,**kw): s.add(ParagraphStyle(name=n,**kw))
    a('T', fontName=FONT_CN_B, fontSize=26, leading=36, alignment=TA_CENTER, spaceAfter=20, textColor=colors.HexColor('#1a237e'))
    a('ST', fontName=FONT_CN, fontSize=14, leading=20, alignment=TA_CENTER, spaceAfter=10, textColor=colors.HexColor('#455a64'))
    a('H1', fontName=FONT_CN_B, fontSize=20, leading=28, spaceBefore=24, spaceAfter=12, textColor=colors.HexColor('#1565c0'))
    a('H2', fontName=FONT_CN_B, fontSize=16, leading=22, spaceBefore=16, spaceAfter=8, textColor=colors.HexColor('#1976d2'))
    a('H3', fontName=FONT_CN_B, fontSize=13, leading=18, spaceBefore=12, spaceAfter=6, textColor=colors.HexColor('#1e88e5'))
    a('B', fontName=FONT_CN, fontSize=10.5, leading=17, spaceBefore=4, spaceAfter=4, alignment=TA_JUSTIFY, textColor=colors.HexColor('#212121'))
    a('BI', fontName=FONT_CN, fontSize=10.5, leading=17, spaceBefore=2, spaceAfter=2, leftIndent=20, textColor=colors.HexColor('#212121'))
    a('BL', fontName=FONT_CN, fontSize=10.5, leading=17, spaceBefore=2, spaceAfter=2, leftIndent=30, bulletIndent=15, textColor=colors.HexColor('#212121'))
    a('Cap', fontName=FONT_CN, fontSize=9, leading=13, alignment=TA_CENTER, spaceAfter=12, textColor=colors.HexColor('#757575'))
    a('CodeBlock', fontName='Courier', fontSize=8, leading=11, spaceBefore=2, spaceAfter=2, leftIndent=20, textColor=colors.HexColor('#37474f'))
    return s

def box(d,x,y,w,h,txt,fc,tc=colors.white,fs=9):
    d.add(Rect(x,y,w,h,rx=5,ry=5,fillColor=fc,strokeColor=colors.HexColor('#90a4ae'),strokeWidth=0.5))
    lines=txt.split('\n')
    sy=y+h/2+(len(lines)*(fs+2))/2-fs/2
    for i,l in enumerate(lines):
        d.add(String(x+w/2,sy-i*(fs+2),l,fontName=FONT_CN,fontSize=fs,fillColor=tc,textAnchor='middle'))

def arrow(d,x1,y1,x2,y2,sc=colors.HexColor('#546e7a'),sw=1.2):
    d.add(Line(x1,y1,x2,y2,strokeColor=sc,strokeWidth=sw))
    a=math.atan2(y2-y1,x2-x1); al=6; aa=math.pi/7
    d.add(Polygon([x2,y2,x2-al*math.cos(a-aa),y2-al*math.sin(a-aa),x2-al*math.cos(a+aa),y2-al*math.sin(a+aa)],fillColor=sc,strokeColor=sc,strokeWidth=0.5))

def label(d,x,y,t,fs=8,c=colors.HexColor('#455a64')):
    d.add(String(x,y,t,fontName=FONT_CN,fontSize=fs,fillColor=c,textAnchor='middle'))

def dashedline(d,x1,y1,x2,y2,sc=colors.HexColor('#90a4ae'),sw=0.8):
    d.add(Line(x1,y1,x2,y2,strokeColor=sc,strokeWidth=sw,strokeDashArray=[4,3]))

def fig_architecture():
    d=Drawing(500,480)
    d.add(Rect(0,0,500,480,fillColor=colors.HexColor('#fafafa'),strokeColor=colors.HexColor('#e0e0e0'),strokeWidth=0.5))
    label(d,250,460,'Flink Models 模块整体架构',13,colors.HexColor('#1565c0'))
    # Flink SQL
    box(d,150,420,200,24,'ML_PREDICT(model, input)',colors.HexColor('#e65100'),fs=9)
    arrow(d,250,420,250,404)
    box(d,100,378,300,24,'org.apache.flink.table.factories.Factory (SPI)',colors.HexColor('#455a64'),fs=8)
    # 两个工厂
    arrow(d,200,378,130,362); arrow(d,300,378,370,362)
    box(d,30,335,200,24,'OpenAIModelProviderFactory',colors.HexColor('#1565c0'),fs=8)
    box(d,270,335,200,24,'TritonModelProviderFactory',colors.HexColor('#2e7d32'),fs=8)
    label(d,130,328,'identifier="openai"',7,colors.HexColor('#1565c0'))
    label(d,370,328,'identifier="triton"',7,colors.HexColor('#2e7d32'))
    # Provider
    arrow(d,130,335,130,316); arrow(d,370,335,370,316)
    box(d,30,290,200,24,'Provider (AsyncPredictRuntimeProvider)',colors.HexColor('#1976d2'),fs=7)
    box(d,270,290,200,24,'Provider (AsyncPredictRuntimeProvider)',colors.HexColor('#388e3c'),fs=7)
    # 具体函数
    arrow(d,80,290,80,272); arrow(d,180,290,180,272)
    box(d,10,245,140,24,'OpenAIChatModel\nFunction',colors.HexColor('#1e88e5'),fs=7)
    box(d,160,245,140,24,'OpenAIEmbedding\nModelFunction',colors.HexColor('#42a5f5'),fs=7)
    arrow(d,370,290,370,272)
    box(d,290,245,160,24,'TritonInference\nModelFunction',colors.HexColor('#43a047'),fs=7)
    # 抽象基类
    dashedline(d,10,235,490,235)
    box(d,30,200,200,28,'AbstractOpenAIModelFunction',colors.HexColor('#7b1fa2'),fs=8)
    box(d,270,200,200,28,'AbstractTritonModelFunction',colors.HexColor('#6a1b9a'),fs=8)
    # HTTP
    dashedline(d,10,190,490,190)
    box(d,30,155,200,28,'OpenAI Java SDK (Async)',colors.HexColor('#c62828'),fs=8)
    box(d,270,155,200,28,'OkHttp + Triton v2 REST',colors.HexColor('#bf360c'),fs=8)
    # 客户端池
    arrow(d,130,155,130,138); arrow(d,370,155,370,138)
    box(d,40,110,180,24,'OpenAIUtils (RefCount Pool)',colors.HexColor('#d32f2f'),fs=7)
    box(d,280,110,180,24,'TritonUtils (RefCount Pool)',colors.HexColor('#e65100'),fs=7)
    # 外部服务
    dashedline(d,10,100,490,100)
    box(d,30,60,200,28,'OpenAI API\n(chat/completions, embeddings)',colors.HexColor('#0d47a1'),fs=7)
    box(d,270,60,200,28,'NVIDIA Triton Server\n(/v2/models/.../infer)',colors.HexColor('#004d40'),fs=7)
    # 辅助
    box(d,30,20,110,28,'jtokkit Token\nCount + Overflow',colors.HexColor('#ff6f00'),fs=6)
    box(d,160,20,110,28,'TritonTypeMapper\nType Mapping',colors.HexColor('#00695c'),fs=6)
    box(d,290,20,90,28,'TritonData\nType Enum',colors.HexColor('#004d40'),fs=6)
    box(d,400,20,90,28,'Exception\nHierarchy',colors.HexColor('#d84315'),fs=6)
    return d

def fig_openai_chat_flow():
    d=Drawing(500,540)
    d.add(Rect(0,0,500,540,fillColor=colors.HexColor('#fafafa'),strokeColor=colors.HexColor('#e0e0e0'),strokeWidth=0.5))
    label(d,250,520,'OpenAI Chat Completion 调用流程',13,colors.HexColor('#1565c0'))
    bw,bh,cx=170,28,150
    steps=[('1. asyncPredict(RowData)','#1565c0'),('2. Null Check','#1976d2'),('3. Token 计数 (jtokkit)','#1e88e5'),
           ('4. 上下文溢出检查','#2196f3'),('5. 截断/跳过处理','#42a5f5'),('6. 构建 ChatCompletionParams','#1e88e5'),
           ('7. 设置可选参数','#1976d2'),('8. client.chat().completions()','#1565c0'),('9. 异步等待响应','#0d47a1'),('10. 提取 choice.message','#002171')]
    notes=['AsyncPredictFunction','null -> return null','jtokkit Encoding','maxContextSize','TRUNCATED/SKIPPED',
           'system + user msg','temp/topP/stop/seed','OpenAI Async SDK','CompletableFuture','GenericRowData']
    for i,(s,c) in enumerate(steps):
        y=480-i*46
        box(d,cx,y,bw,bh,s,colors.HexColor(c),fs=7)
        if i>0: arrow(d,cx+bw/2,y+46,cx+bw/2,y+bh)
        box(d,345,y+1,125,26,notes[i],colors.HexColor('#e3f2fd'),colors.HexColor('#1565c0'),6)
        arrow(d,cx+bw,y+bh/2,345,y+13,colors.HexColor('#90caf9'),0.8)
    return d

def fig_triton_infer_flow():
    d=Drawing(500,540)
    d.add(Rect(0,0,500,540,fillColor=colors.HexColor('#fafafa'),strokeColor=colors.HexColor('#e0e0e0'),strokeWidth=0.5))
    label(d,250,520,'Triton Inference 推理请求流程',13,colors.HexColor('#2e7d32'))
    bw,bh,cx=170,28,150
    steps=[('1. asyncPredict(RowData)','#1b5e20'),('2. 序列化输入 -> JSON','#2e7d32'),('3. 计算 Shape','#388e3c'),
           ('4. 构建 Inference Request','#43a047'),('5. 可选 gzip 压缩','#4caf50'),('6. HTTP POST 请求','#66bb6a'),
           ('7. 解析响应 JSON','#43a047'),('8. 反序列化 output data','#388e3c'),('9. 构建 GenericRowData','#2e7d32'),('10. 错误处理分支','#1b5e20')]
    notes=['TritonInferenceModel','TritonTypeMapper','[1] or [1,N]','inputs + parameters','gzip ByteBuffer',
           '/v2/models/.../infer','outputs[].data','标量/数组反序列化','返回结果RowData','4xx/5xx/Network/Schema']
    for i,(s,c) in enumerate(steps):
        y=480-i*46
        box(d,cx,y,bw,bh,s,colors.HexColor(c),fs=7)
        if i>0: arrow(d,cx+bw/2,y+46,cx+bw/2,y+bh)
        box(d,345,y+1,125,26,notes[i],colors.HexColor('#e8f5e9'),colors.HexColor('#1b5e20'),6)
        arrow(d,cx+bw,y+bh/2,345,y+13,colors.HexColor('#a5d6a7'),0.8)
    return d

def fig_error_handling():
    d=Drawing(500,380)
    d.add(Rect(0,0,500,380,fillColor=colors.HexColor('#fafafa'),strokeColor=colors.HexColor('#e0e0e0'),strokeWidth=0.5))
    label(d,250,360,'错误处理策略 (OpenAI vs Triton)',13,colors.HexColor('#1565c0'))
    # OpenAI
    label(d,130,340,'OpenAI ErrorHandling',9,colors.HexColor('#1565c0'))
    box(d,30,308,200,24,'API 调用失败',colors.HexColor('#c62828'),fs=8)
    arrow(d,60,308,60,290); arrow(d,130,308,130,290); arrow(d,200,308,200,290)
    box(d,10,264,90,22,'RETRY\n(默认100次)',colors.HexColor('#1565c0'),fs=6)
    box(d,110,264,80,22,'FAILOVER\n(直接失败)',colors.HexColor('#c62828'),fs=6)
    box(d,200,264,80,22,'IGNORE\n(返null)',colors.HexColor('#ff9800'),fs=6)
    arrow(d,55,264,55,248)
    box(d,10,222,90,22,'重试耗尽?\nFallback',colors.HexColor('#e3f2fd'),colors.HexColor('#1565c0'),fs=6)
    box(d,110,222,170,22,'ErrorMetadata: error-string / status-code / headers',colors.HexColor('#f3e5f5'),colors.HexColor('#6a1b9a'),fs=5)
    # Triton
    dashedline(d,10,205,490,205)
    label(d,370,340,'Triton Exception Hierarchy',9,colors.HexColor('#2e7d32'))
    box(d,310,308,170,24,'TritonException (基类)',colors.HexColor('#2e7d32'),fs=8)
    arrow(d,340,308,340,290); arrow(d,395,308,395,290); arrow(d,450,308,450,290)
    box(d,300,264,85,22,'Client\n4xx',colors.HexColor('#e65100'),fs=6)
    box(d,390,264,85,22,'Server\n5xx',colors.HexColor('#c62828'),fs=6)
    box(d,300,234,85,22,'Network\nIOException',colors.HexColor('#0d47a1'),fs=6)
    box(d,390,234,85,22,'Schema\nShape错误',colors.HexColor('#4a148c'),fs=6)
    # ErrorCategory
    box(d,310,185,170,35,'ErrorCategory:\nCLIENT | SERVER | NETWORK\nSCHEMA | UNKNOWN',colors.HexColor('#004d40'),fs=6)
    # retryable
    box(d,310,145,170,30,'isRetryable:\nNetwork=true, Server503/504=true\nClient=false, Schema=false',colors.HexColor('#e8f5e9'),colors.HexColor('#1b5e20'),fs=6)
    return d

def fig_type_mapping():
    d=Drawing(500,350)
    d.add(Rect(0,0,500,350,fillColor=colors.HexColor('#fafafa'),strokeColor=colors.HexColor('#e0e0e0'),strokeWidth=0.5))
    label(d,250,330,'Flink <-> Triton 数据类型映射',13,colors.HexColor('#2e7d32'))
    mappings=[('BooleanType','BOOL'),('TinyIntType','INT8'),('SmallIntType','INT16'),('IntType','INT32'),
              ('BigIntType','INT64'),('FloatType','FP32'),('DoubleType','FP64'),('VarCharType','BYTES'),('ArrayType<T>','递归取元素类型')]
    for i,(f,t) in enumerate(mappings):
        y=300-i*30
        box(d,20,y,160,22,f,colors.HexColor('#1565c0'),fs=8)
        box(d,320,y,160,22,t,colors.HexColor('#2e7d32'),fs=8)
        arrow(d,180,y+11,320,y+11,colors.HexColor('#90a4ae'),1.0)
    label(d,250,18,'Shape: 标量->[batch], 数组->[batch,size], flattenBatchDim: [1,N]->[N]',8,colors.HexColor('#455a64'))
    return d

def fig_client_pool():
    d=Drawing(500,320)
    d.add(Rect(0,0,500,320,fillColor=colors.HexColor('#fafafa'),strokeColor=colors.HexColor('#e0e0e0'),strokeWidth=0.5))
    label(d,250,300,'引用计数 HTTP 客户端池 (共享模式)',13,colors.HexColor('#1565c0'))
    # Subtasks
    for i in range(4):
        box(d,15+i*80,250,70,24,f'Subtask {i}',colors.HexColor('#e65100'),fs=7)
        arrow(d,50+i*80,250,190 if i<2 else 370,235)
    # 池
    box(d,100,205,180,26,'OpenAIUtils Pool\nKey=(baseUrl,apiKey)',colors.HexColor('#1565c0'),fs=7)
    box(d,310,205,180,26,'TritonUtils Pool\nKey=(timeoutMs)',colors.HexColor('#2e7d32'),fs=7)
    # RefCount
    arrow(d,190,205,190,185); arrow(d,400,205,400,185)
    box(d,100,160,180,22,'ReferenceValue(client,refCount)',colors.HexColor('#7b1fa2'),fs=7)
    box(d,310,160,180,22,'ReferenceValue(client,refCount)',colors.HexColor('#6a1b9a'),fs=7)
    # HTTP Client
    arrow(d,190,160,190,140); arrow(d,400,160,400,140)
    box(d,100,115,180,22,'OpenAIClientAsync',colors.HexColor('#c62828'),fs=8)
    box(d,310,115,180,22,'OkHttpClient',colors.HexColor('#bf360c'),fs=8)
    # 生命周期
    dashedline(d,10,100,490,100)
    box(d,20,55,130,30,'open()\ncreateClient()\nrefCount++',colors.HexColor('#2e7d32'),fs=7)
    box(d,175,55,130,30,'close()\nreleaseClient()\nrefCount--',colors.HexColor('#ff9800'),fs=7)
    box(d,330,55,150,30,'refCount==0?\nshutdown+evict',colors.HexColor('#c62828'),fs=7)
    arrow(d,150,70,175,70); arrow(d,305,70,330,70)
    return d

def fig_context_overflow():
    d=Drawing(500,340)
    d.add(Rect(0,0,500,340,fillColor=colors.HexColor('#fafafa'),strokeColor=colors.HexColor('#e0e0e0'),strokeWidth=0.5))
    label(d,250,320,'OpenAI 上下文溢出处理策略',13,colors.HexColor('#1565c0'))
    box(d,170,280,160,24,'输入文本 -> jtokkit 编码',colors.HexColor('#1565c0'),fs=8)
    arrow(d,250,280,250,262)
    box(d,160,236,180,24,'tokenCount > maxContextSize?',colors.HexColor('#e3f2fd'),colors.HexColor('#1565c0'),fs=8)
    arrow(d,340,248,400,248); box(d,400,236,80,24,'直接发送',colors.HexColor('#2e7d32'),fs=8)
    arrow(d,250,236,250,218)
    strategies=[('TRUNCATED_TAIL','尾部截断'),('TRUNCATED_HEAD','头部截断'),('SKIPPED','跳过(返null)'),
                ('TRUNCATED_TAIL_LOG','尾截+WARN'),('TRUNCATED_HEAD_LOG','头截+WARN'),('SKIPPED_LOG','跳过+WARN')]
    for i,(n,d2) in enumerate(strategies):
        col=i%3; row=i//3
        x=20+col*165; y=170-row*50
        box(d,x,y,155,38,f'{n}\n{d2}',colors.HexColor('#9c27b0'),fs=6)
    box(d,30,60,200,30,'HeadTrimmedIntArrayList\noffset实现零拷贝头部裁剪',colors.HexColor('#4a148c'),fs=7)
    box(d,260,60,220,30,'jtokkit: text->tokens->截断\n->decode->truncated text',colors.HexColor('#0d47a1'),fs=7)
    return d

def fig_async_sequence():
    d=Drawing(500,400)
    d.add(Rect(0,0,500,400,fillColor=colors.HexColor('#fafafa'),strokeColor=colors.HexColor('#e0e0e0'),strokeWidth=0.5))
    label(d,250,380,'ML_PREDICT 异步预测时序图',13,colors.HexColor('#1565c0'))
    pts=[(80,'Flink Runtime','#e65100'),(250,'ModelFunction','#1565c0'),(420,'External Service','#2e7d32')]
    for x,n,c in pts:
        box(d,x-55,350,110,22,n,colors.HexColor(c),fs=8)
        d.add(Line(x,350,x,20,strokeColor=colors.HexColor('#e0e0e0'),strokeWidth=0.8))
    y=330; sh=35
    box(d,20,y,120,18,'open() 初始化',colors.HexColor('#e65100'),fs=7)
    arrow(d,140,y+9,190,y+9); box(d,190,y,120,18,'创建 HTTP Client',colors.HexColor('#1565c0'),fs=7)
    y-=sh
    box(d,20,y,120,18,'asyncPredict(row)',colors.HexColor('#f57c00'),fs=7)
    arrow(d,140,y+9,190,y+9); box(d,190,y,120,18,'输入校验+预处理',colors.HexColor('#1976d2'),fs=7)
    y-=sh
    box(d,190,y,120,18,'构建请求参数',colors.HexColor('#1e88e5'),fs=7)
    arrow(d,310,y+9,360,y+9); box(d,360,y+0,120,18,'HTTP POST',colors.HexColor('#2e7d32'),fs=7)
    y-=sh
    box(d,360,y,120,18,'模型推理处理',colors.HexColor('#388e3c'),fs=7)
    y-=sh
    arrow(d,360,y+9,310,y+9,colors.HexColor('#66bb6a'))
    box(d,190,y,120,18,'解析响应',colors.HexColor('#1565c0'),fs=7)
    y-=sh
    arrow(d,190,y+9,140,y+9,colors.HexColor('#42a5f5'))
    box(d,20,y,120,18,'Collection<RowData>',colors.HexColor('#ff9800'),fs=7)
    y-=sh
    dashedline(d,10,y+22,490,y+22)
    label(d,250,y+10,'错误场景',9,colors.HexColor('#c62828'))
    box(d,360,y,120,18,'返回错误',colors.HexColor('#c62828'),fs=7)
    arrow(d,360,y+9,310,y+9,colors.HexColor('#ef5350'))
    box(d,190,y,120,18,'handleErrors()',colors.HexColor('#d32f2f'),fs=7)
    y-=sh
    box(d,190,y,120,18,'RETRY/FAILOVER',colors.HexColor('#b71c1c'),fs=7)
    arrow(d,190,y+9,140,y+9,colors.HexColor('#ef5350'))
    box(d,20,y,120,18,'异常或空结果',colors.HexColor('#c62828'),fs=7)
    return d

def fig_class_hierarchy():
    d=Drawing(500,380)
    d.add(Rect(0,0,500,380,fillColor=colors.HexColor('#fafafa'),strokeColor=colors.HexColor('#e0e0e0'),strokeWidth=0.5))
    label(d,250,360,'核心类层次结构',13,colors.HexColor('#1565c0'))
    bw=175
    layers=[('SPI 工厂层','#e65100',[('ModelProviderFactory','#fff3e0','#e65100'),('OpenAI/Triton Factory','#e65100','#ffffff')]),
            ('Provider 层','#2e7d32',[('AsyncPredictRuntime','#e8f5e9','#2e7d32'),('Provider (内部类)','#2e7d32','#ffffff')]),
            ('异步预测层','#1565c0',[('AsyncPredictFunction','#e3f2fd','#1565c0'),('Abstract*ModelFunc','#1565c0','#ffffff')]),
            ('具体实现层','#6a1b9a',[('Chat/Embedding Func','#f3e5f5','#6a1b9a'),('TritonInferenceFunc','#6a1b9a','#ffffff')]),
            ('HTTP客户端层','#c62828',[('OpenAIClientAsync','#ffebee','#c62828'),('OkHttpClient','#c62828','#ffffff')]),
            ('配置选项层','#00695c',[('OpenAIOptions','#e0f2f1','#00695c'),('TritonOptions','#00695c','#ffffff')])]
    for i,(lbl,lc,cls) in enumerate(layers):
        y=325-i*50
        label(d,55,y+5,lbl,8,colors.HexColor(lc))
        for j,(cn,bg,tc) in enumerate(cls):
            box(d,10+j*200,y-18,bw,20,cn,colors.HexColor(bg),colors.HexColor(tc),7)
        if len(cls)==2: arrow(d,10+bw,y-8,210,y-8)
    return d

def _tbl(data, cw, hdr_color='#1565c0'):
    t=Table(data, colWidths=cw)
    t.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor(hdr_color)),('TEXTCOLOR',(0,0),(-1,0),colors.white),
        ('FONTNAME',(0,0),(-1,-1),FONT_CN),('FONTSIZE',(0,0),(-1,-1),7),('ALIGN',(0,0),(-1,-1),'LEFT'),
        ('GRID',(0,0),(-1,-1),0.5,colors.HexColor('#b0bec5')),
        ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.HexColor('#fafafa'),colors.HexColor('#f5f5f5')]),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),('TOPPADDING',(0,0),(-1,-1),3),('BOTTOMPADDING',(0,0),(-1,-1),3)]))
    return t

def build():
    out=os.path.join(os.path.dirname(os.path.abspath(__file__)),'Flink_Models_模块源码分析.pdf')
    doc=SimpleDocTemplate(out,pagesize=A4,rightMargin=2*cm,leftMargin=2*cm,topMargin=2.5*cm,bottomMargin=2*cm)
    st=mkstyles(); story=[]
    def P(t,s): story.append(Paragraph(t,st[s]))
    def BL(items):
        for r in items: P(f'  \u2022 {r}','BL')
    def HR(): story.append(HRFlowable(width="100%",thickness=1,color=colors.HexColor('#e0e0e0')))
    def SP(h=10): story.append(Spacer(1,h))

    # 封面
    story+=[Spacer(1,80),Paragraph('Flink Models 模块',st['T']),Paragraph('核心流程与设计说明',st['T']),
            Spacer(1,20),HRFlowable(width="60%",thickness=2,color=colors.HexColor('#1565c0')),Spacer(1,20),
            Paragraph('基于 Apache Flink 2.3-SNAPSHOT 源码分析',st['ST']),
            Paragraph('flink-models 模块完整解读 (OpenAI + Triton)',st['ST']),Spacer(1,40),
            Paragraph('模块路径: flink-models/{flink-model-openai, flink-model-triton}',st['ST']),
            Paragraph('共 18 个核心 Java 源文件 | 25 个 Java 文件(含测试)',st['ST']),PageBreak()]

    # 目录
    P('目录','H1'); SP()
    for t in ['一、模块概览与架构设计','二、核心类设计说明','三、OpenAI Chat Completion 调用流程',
              '四、Triton Inference 推理请求流程','五、ML_PREDICT 异步预测时序图',
              '六、错误处理策略','七、上下文溢出处理 (OpenAI)','八、Flink-Triton 类型映射',
              '九、引用计数客户端池','十、参数与配置体系','十一、SPI 机制与扩展点','十二、关键设计模式']:
        P(t,'B'); SP(3)
    story.append(PageBreak())

    # === 第一章 ===
    P('一、模块概览与架构设计','H1'); HR()
    P('1.1 模块定位','H2')
    P('flink-models 是 Apache Flink 2.3 版本新增的模型推理服务集成模块，旨在通过 Flink SQL 的 ML_PREDICT 函数将外部 AI/ML 模型推理能力无缝嵌入流处理管道。该模块采用 Maven 多模块结构，包含两个子模块: flink-model-openai (集成 OpenAI API，支持 Chat Completion 和 Embedding) 和 flink-model-triton (集成 NVIDIA Triton Inference Server，支持通用模型推理)。两个子模块均基于 Flink 的 AsyncPredictFunction 异步预测框架，通过 SPI 机制自动注册为 Flink Table Factory。','B')
    P('1.2 模块包结构','H2')
    story.append(_tbl([['子模块','包名','核心类数','职责'],
        ['flink-model-openai','o.a.f.model.openai','7','OpenAI API 集成 (Chat + Embedding)'],
        ['flink-model-triton','o.a.f.model.triton','7','Triton Inference Server 集成'],
        ['flink-model-triton','o.a.f.model.triton.exception','5','Triton 异常层次体系']],[100,130,50,160]))
    SP()
    P('1.3 依赖关系','H2')
    story.append(_tbl([['子模块','关键依赖','版本','用途'],
        ['openai','com.openai:openai-java','1.6.1','OpenAI 官方 Java SDK (异步)'],
        ['openai','com.knuddels:jtokkit','1.1.0','Token 编码计数'],
        ['triton','com.squareup.okhttp3:okhttp','4.12.0','HTTP 客户端'],
        ['triton','jackson-core/databind','2.15.2','JSON 序列化/反序列化'],
        ['共用','flink-core/table-api-java','2.3-SNAPSHOT','Flink 核心 (provided)']],[80,140,50,170]))
    SP()
    P('1.4 核心类层次结构','H2')
    story+=[fig_class_hierarchy(),Paragraph('图 1-1: 核心类层次结构',st['Cap'])]
    P('1.5 整体架构概览','H2')
    story+=[fig_architecture(),Paragraph('图 1-2: Flink Models 模块整体架构',st['Cap']),PageBreak()]

    # === 第二章 ===
    P('二、核心类设计说明','H1'); HR()
    P('2.1 OpenAIModelProviderFactory (SPI 入口, 约107行)','H2')
    P('实现 ModelProviderFactory 接口，SPI 标识符为 "openai"。根据 endpoint URL 后缀自动路由到不同的 ModelFunction 实现。','B')
    BL(['factoryIdentifier(): 返回 "openai"，用于 Flink Table Factory SPI 自动发现',
        '路由逻辑: endpoint 以 "chat/completions" 结尾 -> OpenAIChatModelFunction; 以 "embeddings" 结尾 -> OpenAIEmbeddingModelFunction',
        '创建 Provider 内部类: 实现 AsyncPredictRuntimeProvider，持有具体的 AsyncPredictFunction 实例',
        '必选参数: endpoint, api-key, model; 可选: max-context-size, error-handling-strategy, system-prompt, temperature 等'])
    P('2.2 AbstractOpenAIModelFunction (抽象基类, 约338行)','H2')
    P('继承 AsyncPredictFunction，管理 HTTP 客户端生命周期、Token 上下文控制和错误处理。','B')
    BL(['open(): 通过 OpenAIUtils 获取引用计数的 OpenAIClientAsync + 初始化 jtokkit Tokenizer',
        'close(): 释放客户端引用 (引用计数减一)',
        'asyncPredict(): 模板方法 — null 检查 -> token 限制 -> 调用子类 asyncPredictInternal()',
        'handleErrorsAndRespond(): FAILOVER(抛异常) / IGNORE(返空+元数据)',
        '内部枚举: ErrorHandlingStrategy(RETRY/FAILOVER/IGNORE), RetryFallbackStrategy(FAILOVER/IGNORE)',
        'ErrorMessageMetadata: error-string(STRING) / http-status-code(INT) / http-headers-map(MAP)'])
    P('2.3 OpenAIChatModelFunction (Chat 完成, 约160行)','H2')
    P('继承 AbstractOpenAIModelFunction，实现 OpenAI Chat Completion API 调用。输入: 单列 STRING; 输出: 单列 STRING。','B')
    BL(['构建 ChatCompletionCreateParams: system prompt + user message + model',
        '可选参数: temperature, topP, stop, maxTokens, presencePenalty, n, seed, responseFormat(TEXT/JSON_OBJECT)',
        '响应处理: 提取每个 choice 的 message.content 生成 GenericRowData'])
    P('2.4 OpenAIEmbeddingModelFunction (向量嵌入, 约116行)','H2')
    P('输入: 单列 STRING; 输出: 单列 ARRAY&lt;FLOAT&gt; (嵌入向量)。构建 EmbeddingCreateParams，将 embedding 向量转为 GenericArrayData。','B')
    P('2.5 TritonModelProviderFactory (SPI 入口, 约95行)','H2')
    P('SPI 标识符 "triton"。必选: endpoint, model-name; 可选: model-version, timeout, flatten-batch-dim, priority, sequence-id, compression, auth-token, custom-headers。','B')
    P('2.6 TritonInferenceModelFunction (核心推理, 约455行)','H2')
    P('继承 AbstractTritonModelFunction，实现 Triton v2 REST API 推理。这是 Triton 子模块最核心的类。','B')
    BL(['asyncPredict(): 构建请求 -> HTTP POST -> 解析响应 -> 返回 RowData',
        'buildInferenceRequest(): 序列化 -> 计算shape -> flattenBatchDim -> sequence/priority',
        'URL: {endpoint}/v2/models/{name}/versions/{version}/infer',
        'gzip 压缩: 可复用 ByteArrayOutputStream 缓冲区',
        'handleErrorResponse(): 400->SchemaException, 404->检查模型名, 401/403->检查认证, 5xx->ServerException'])
    P('2.7 AbstractTritonModelFunction (抽象基类, 约323行)','H2')
    BL(['open()/close(): 管理 OkHttpClient 引用计数',
        'validateSingleColumnSchema(): 确保单列 + Triton 类型兼容',
        '禁止嵌套数组 (ARRAY&lt;ARRAY&lt;T&gt;&gt;)，提供类型转换建议'])
    P('2.8 其他核心类','H2')
    story.append(_tbl([['类名','职责'],
        ['ContextOverflowAction','上下文溢出枚举(6种): TRUNCATED_TAIL/HEAD + SKIPPED (各有+LOG变体)'],
        ['OpenAIUtils','引用计数池: Key=(baseUrl,apiKey), 复用 OpenAIClientAsync'],
        ['TritonDataType','Triton 数据类型枚举(13种): BOOL, INT8-64, FP16-64, BYTES'],
        ['TritonTypeMapper','Flink<->Triton 类型映射 + 序列化/反序列化 + Shape计算'],
        ['TritonUtils','引用计数池: Key=timeoutMs + buildInferenceUrl()'],
        ['TritonException','异常基类: isRetryable() + getCategory() + ErrorCategory'],
        ['TritonClientException','4xx, isRetryable=false'],
        ['TritonServerException','5xx, 503/504 可重试'],
        ['TritonNetworkException','IOException, isRetryable=true'],
        ['TritonSchemaException','Shape不匹配, expected vs actual']],[110,330]))
    story.append(PageBreak())

    # === 第三章 ===
    P('三、OpenAI Chat Completion 调用流程','H1'); HR()
    P('OpenAI Chat Completion 是 flink-model-openai 模块最核心的功能。通过 OpenAIChatModelFunction 将用户文本发送到 OpenAI API，获取 AI 生成的回复文本。','B')
    story+=[fig_openai_chat_flow(),Paragraph('图 3-1: OpenAI Chat Completion 调用流程',st['Cap'])]
    P('关键实现细节:','H3')
    BL(['asyncPredict() 首先检查输入是否为 null，若为 null 直接返回 null',
        'Token 计数使用 jtokkit 的 Encoding 对象编码为 token ID 数组后计数',
        '超过 maxContextSize 时按 ContextOverflowAction 策略截断/跳过 (默认 TRUNCATED_TAIL)',
        '构建 ChatCompletionCreateParams: system prompt (可选) + user message + model',
        'responseFormat 支持 TEXT 和 JSON_OBJECT 两种模式',
        '异步调用返回 CompletableFuture，提取每个 choice 的 message.content'])
    story.append(PageBreak())

    # === 第四章 ===
    P('四、Triton Inference 推理请求流程','H1'); HR()
    P('通过 TritonInferenceModelFunction 将 Flink 数据发送到 NVIDIA Triton Inference Server 执行模型推理。采用 Triton v2 REST API 协议。','B')
    story+=[fig_triton_infer_flow(),Paragraph('图 4-1: Triton Inference 推理请求流程',st['Cap'])]
    P('关键实现细节:','H3')
    BL(['TritonTypeMapper.serializeToJsonArray() 序列化为 JSON 数组 (数组类型展平)',
        'Shape: 标量->[1], 数组->[1,N]; flattenBatchDim=true 时 [1,N]->[N]',
        '请求 JSON: {"inputs":[{"name":..,"datatype":..,"shape":..,"data":..}],"parameters":{..}}',
        'gzip 压缩使用可复用缓冲区; 支持 Bearer Token + 自定义 Headers',
        '响应: outputs[0].data 标量取首元素/数组完整反序列化',
        'sequence-id/start/end 参数支持有状态模型推理'])
    story.append(PageBreak())

    # === 第五章 ===
    P('五、ML_PREDICT 异步预测时序图','H1'); HR()
    P('Flink SQL 的 ML_PREDICT 函数触发异步预测。基于 CompletableFuture 实现非阻塞异步执行。','B')
    story+=[fig_async_sequence(),Paragraph('图 5-1: ML_PREDICT 异步预测时序图',st['Cap'])]
    BL(['open(): 通过引用计数池获取 HTTP 客户端 + 初始化 Tokenizer/TypeMapper',
        'asyncPredict(): RowData -> 预处理 -> 构建请求 -> 异步 HTTP -> 解析响应',
        '错误处理: OpenAI 按 ErrorHandlingStrategy; Triton 抛出分类异常',
        'close(): 释放客户端引用，归零时关闭连接'])
    story.append(PageBreak())

    # === 第六章 ===
    P('六、错误处理策略','H1'); HR()
    P('OpenAI 提供可配置三级策略+元数据回传; Triton 实现完整异常层次+可重试判断。','B')
    story+=[fig_error_handling(),Paragraph('图 6-1: 错误处理策略对比',st['Cap'])]
    P('6.1 OpenAI 错误处理','H3')
    BL(['RETRY(默认): 自动重试 retry-num 次(默认100)',
        'FAILOVER: 直接抛 RuntimeException 触发 Flink Failover',
        'IGNORE: 忽略错误返回 null + ErrorMessageMetadata 元数据',
        'RetryFallbackStrategy: 重试耗尽后 FAILOVER(默认)/IGNORE'])
    P('6.2 Triton 异常层次','H3')
    story.append(_tbl([['异常类','触发条件','isRetryable','ErrorCategory'],
        ['TritonClientException','4xx','false','CLIENT_ERROR'],
        ['TritonServerException','5xx','503/504:true','SERVER_ERROR'],
        ['TritonNetworkException','IOException','true','NETWORK_ERROR'],
        ['TritonSchemaException','Shape/Type不匹配','false','SCHEMA_ERROR']],[110,100,75,100],'#2e7d32'))
    SP()
    BL(['400 Bad Request: 含 shape 关键词 -> TritonSchemaException',
        '404: 提示检查 model-name/version; 401/403: 提示检查 auth-token',
        '503/504: isRetryable=true; 其他 5xx: isRetryable=false'])
    story.append(PageBreak())

    # === 第七章 ===
    P('七、上下文溢出处理 (OpenAI)','H1'); HR()
    P('通过 jtokkit 库进行 Token 编码和计数，超过 max-context-size 时按策略截断/跳过。','B')
    story+=[fig_context_overflow(),Paragraph('图 7-1: 上下文溢出处理策略',st['Cap'])]
    story.append(_tbl([['枚举值','行为','日志'],
        ['TRUNCATED_TAIL','从尾部截断: 保留前 N 个 token','无'],
        ['TRUNCATED_TAIL_LOG','同上','WARN'],
        ['TRUNCATED_HEAD','从头部截断: 保留后 N 个 token','无'],
        ['TRUNCATED_HEAD_LOG','同上','WARN'],
        ['SKIPPED','跳过该行返回 null','无'],
        ['SKIPPED_LOG','同上','WARN']],[130,200,50],'#7b1fa2'))
    SP()
    P('HeadTrimmedIntArrayList 优化: 继承 IntArrayList，通过 offset 实现零拷贝头部裁剪。size()=originalSize-offset, get(i)=array[i+offset]。','B')
    story.append(PageBreak())

    # === 第八章 ===
    P('八、Flink-Triton 类型映射','H1'); HR()
    P('TritonTypeMapper 实现 Flink LogicalType 与 Triton DataType 的双向映射、序列化和反序列化。','B')
    story+=[fig_type_mapping(),Paragraph('图 8-1: Flink-Triton 数据类型映射',st['Cap'])]
    P('8.1 序列化 (Flink -> Triton)','H3')
    BL(['标量: 直接序列化为 [value]; 数组: 展平为 [v1,v2,..,vN]',
        'VarChar/String 编码为 UTF-8 对应 Triton BYTES 类型',
        '使用原始类型数组 (int[],float[]) 避免装箱开销'])
    P('8.2 反序列化 (Triton -> Flink)','H3')
    BL(['标量: 从 data 取首元素; 数组: 完整反序列化为 ArrayData',
        '整数用 Number.intValue()/longValue(); 浮点用 floatValue()/doubleValue()'])
    P('8.3 Shape 计算','H3')
    P('calculateShape(): 标量->[batchSize], 数组->[batchSize,arraySize]。flattenBatchDim=true 且 batch=1 时 [1,N]->[N]。','B')
    story.append(PageBreak())

    # === 第九章 ===
    P('九、引用计数客户端池','H1'); HR()
    P('OpenAIUtils 和 TritonUtils 实现引用计数 HTTP 客户端池，高并行度下多 Subtask 共享同一客户端。','B')
    story+=[fig_client_pool(),Paragraph('图 9-1: 引用计数 HTTP 客户端池',st['Cap'])]
    P('9.1 OpenAIUtils','H3')
    BL(['Key=(baseUrl,apiKey); createAsyncClient(): 命中refCount++ / 未命中创建新客户端',
        'releaseAsyncClient(): refCount-- 归零关闭; synchronized 线程安全'])
    P('9.2 TritonUtils','H3')
    BL(['Key=timeoutMs; connect/read/write 超时统一; 开启连接失败重试',
        'releaseHttpClient(): 归零时 shutdown dispatcher + evict 连接池',
        'buildInferenceUrl(): 智能 URL 规范化，自动补全 /v2/models/.../infer'])
    story.append(PageBreak())

    # === 第十章 ===
    P('十、参数与配置体系','H1'); HR()
    P('10.1 OpenAI 配置项 (OpenAIOptions)','H2')
    story.append(_tbl([['分类','配置项','类型','默认值','说明'],
        ['通用','endpoint','String','必选','OpenAI API URL'],
        ['通用','api-key','String','必选','API 密钥'],
        ['通用','model','String','必选','模型名(gpt-3.5等)'],
        ['上下文','max-context-size','Integer','无','最大 token 数'],
        ['上下文','context-overflow-action','Enum','TRUNCATED_TAIL','溢出策略'],
        ['错误','error-handling-strategy','Enum','RETRY','错误策略'],
        ['错误','retry-num','Integer','100','重试次数'],
        ['错误','retry-fallback-strategy','Enum','FAILOVER','重试耗尽回退'],
        ['Chat','system-prompt','String','无','系统提示词'],
        ['Chat','temperature','Double','无','温度(0-2)'],
        ['Chat','top-p','Double','无','核采样'],
        ['Chat','max-tokens','Integer','无','最大生成token'],
        ['Chat','n','Integer','无','候选数'],
        ['Chat','response-format','Enum','无','TEXT/JSON_OBJECT'],
        ['Embed','dimension','Integer','无','向量维度']],[40,115,42,68,175]))
    SP()
    P('10.2 Triton 配置项 (TritonOptions)','H2')
    story.append(_tbl([['分类','配置项','类型','默认值','说明'],
        ['基础','endpoint','String','必选','Triton Server URL'],
        ['基础','model-name','String','必选','模型名称'],
        ['基础','model-version','String','latest','模型版本'],
        ['基础','timeout','Duration','30s','HTTP超时'],
        ['高级','flatten-batch-dim','Boolean','false','[1,N]->[N]'],
        ['高级','priority','Integer','无','优先级(0-255)'],
        ['序列','sequence-id','String','无','序列ID'],
        ['序列','sequence-start','Boolean','false','序列开始'],
        ['序列','sequence-end','Boolean','false','序列结束'],
        ['HTTP','compression','String','无','gzip'],
        ['HTTP','auth-token','String','无','Bearer Token'],
        ['HTTP','custom-headers','Map','无','自定义头']],[40,100,48,48,200],'#2e7d32'))
    story.append(PageBreak())

    # === 第十一章 ===
    P('十一、SPI 机制与扩展点','H1'); HR()
    P('11.1 SPI 注册','H2')
    P('两个子模块通过 META-INF/services/org.apache.flink.table.factories.Factory 文件注册:','B')
    BL(['flink-model-openai: org.apache.flink.model.openai.OpenAIModelProviderFactory',
        'flink-model-triton: org.apache.flink.model.triton.TritonModelProviderFactory'])
    P('11.2 Shade 机制','H2')
    P('两个模块均使用 maven-shade-plugin 对第三方依赖进行 relocation，避免与用户依赖冲突:','B')
    BL(['OpenAI: jackson -> org.apache.flink.model.openai.shaded.jackson; httpcomponents -> shaded.httpcomponents',
        'Triton: jackson -> org.apache.flink.model.triton.shaded.jackson; squareup -> shaded.squareup'])
    P('11.3 扩展方式','H2')
    BL(['新增 Model Provider: 实现 ModelProviderFactory + AsyncPredictFunction，注册 SPI 服务文件',
        'TritonModelProviderFactory 预留扩展: 未来可根据模型类型路由到不同 ModelFunction',
        'AbstractTritonModelFunction 未来路线图: 支持 gRPC 协议、批量推理、多输入多输出'])
    story.append(PageBreak())

    # === 第十二章 ===
    P('十二、关键设计模式与架构思想','H1'); HR()
    patterns=[
        ('工厂方法模式 (Factory Method)','OpenAIModelProviderFactory 和 TritonModelProviderFactory 实现 ModelProviderFactory 接口，通过 SPI 自动发现。工厂根据配置参数创建具体的 AsyncPredictFunction 实例，解耦实例化逻辑。OpenAI 工厂还包含 URL 路由逻辑，自动选择 Chat 或 Embedding 实现。'),
        ('模板方法模式 (Template Method)','AbstractOpenAIModelFunction.asyncPredict() 定义骨架流程 (null检查 -> token控制 -> 调用子类)，子类 OpenAIChatModelFunction 和 OpenAIEmbeddingModelFunction 实现 asyncPredictInternal() 具体逻辑。AbstractTritonModelFunction 同理。'),
        ('策略模式 (Strategy)','ErrorHandlingStrategy 枚举(RETRY/FAILOVER/IGNORE) 允许用户配置不同的错误处理策略。ContextOverflowAction 枚举(6种) 控制上下文溢出时的处理行为。两者均通过配置项动态选择。'),
        ('异步编程模式 (Async)','全部基于 AsyncPredictFunction，返回 CompletableFuture。OpenAI 使用官方 SDK 的异步 API；Triton 使用 OkHttp 的 enqueue() 异步回调。实现非阻塞的模型推理调用。'),
        ('对象池模式 (Object Pool)','OpenAIUtils 和 TritonUtils 实现引用计数的 HTTP 客户端池。相同配置的多个 Subtask 共享同一客户端实例，避免高并行度下创建过多连接。synchronized 保证线程安全。'),
        ('SPI 服务发现','两个工厂通过 META-INF/services 注册为 Flink Table Factory，实现插件化模型提供者。用户通过 CREATE MODEL 语句指定 provider，Flink 自动通过 SPI 发现对应工厂。'),
        ('适配器模式 (Adapter)','TritonTypeMapper 作为类型适配器，将 Flink LogicalType 转换为 Triton DataType，并实现双向序列化/反序列化。弥合了两个系统之间的类型差异。'),
        ('分层异常体系','Triton 模块设计了 TritonException 基类和 4 个子类的异常层次，每个异常携带 isRetryable() 和 ErrorCategory 信息。使上层可以根据异常类型做精确的重试和降级决策。'),
    ]
    for name,desc in patterns:
        P(f'<b>{name}</b>','H3'); P(desc,'BI')

    story+=[Spacer(1,30),HRFlowable(width="100%",thickness=2,color=colors.HexColor('#1565c0'))]
    P('文档结束 — 基于 Flink 2.3-SNAPSHOT flink-models 模块全部源码分析生成','Cap')

    doc.build(story)
    print(f"PDF generated: {out}")
    return out

if __name__=='__main__':
    build()
