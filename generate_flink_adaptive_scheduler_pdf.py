#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Flink AdaptiveScheduler 核心架构设计文档 PDF 生成器"""
import os, math
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.lib.colors import HexColor, black, white
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, PageBreak, Table, TableStyle, Flowable)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

CB=HexColor('#1A237E');C1=HexColor('#1565C0');C2=HexColor('#0277BD')
C3=HexColor('#00838F');CG=HexColor('#2E7D32');CO=HexColor('#E65100')
CP=HexColor('#6A1B9A');CR=HexColor('#C62828');CGY=HexColor('#757575')
CLB=HexColor('#E3F2FD');CLG=HexColor('#E8F5E9');CLO=HexColor('#FFF3E0')
CLP=HexColor('#F3E5F5');CLY=HexColor('#FFF8E1');CLGY=HexColor('#F5F5F5')
CBD=HexColor('#BDBDBD');CT3=HexColor('#AD1457');CT2=HexColor('#4527A0');CT1=HexColor('#00695C')

def register_fonts():
    for p in ['/System/Library/Fonts/PingFang.ttc','/System/Library/Fonts/STHeiti Light.ttc','/System/Library/Fonts/Hiragino Sans GB.ttc','/Library/Fonts/Arial Unicode.ttf']:
        if os.path.exists(p):
            try:
                pdfmetrics.registerFont(TTFont('CF',p,subfontIndex=0))
                try: pdfmetrics.registerFont(TTFont('CFB',p,subfontIndex=1))
                except: pdfmetrics.registerFont(TTFont('CFB',p,subfontIndex=0))
                return True
            except: continue
    pdfmetrics.registerAlias('CF','Helvetica');pdfmetrics.registerAlias('CFB','Helvetica-Bold')
    return False

class FC(Flowable):
    def __init__(s,w,h,fn): Flowable.__init__(s);s.width=w;s.height=h;s.fn=fn
    def draw(s): s.fn(s.canv,s.width,s.height)

def rr(c,x,y,w,h,r,fc,text="",fs=8):
    c.saveState();c.setFillColor(fc);c.setStrokeColor(fc);c.setLineWidth(1)
    c.roundRect(x,y,w,h,r,stroke=1,fill=1)
    if text:
        il=fc in(CLB,CLG,CLO,CLP,CLY,white,CLGY)
        c.setFillColor(black if il else white);c.setFont('CF',fs)
        ls=text.split('\n');th=len(ls)*(fs+2);sy=y+h/2+th/2-fs
        for i,l in enumerate(ls):
            tw=c.stringWidth(l,'CF',fs);c.drawString(x+(w-tw)/2,sy-i*(fs+2),l)
    c.restoreState()

def ar(c,x1,y1,x2,y2,clr=black):
    c.saveState();c.setStrokeColor(clr);c.setFillColor(clr);c.setLineWidth(1.2)
    c.line(x1,y1,x2,y2);a=math.atan2(y2-y1,x2-x1);al=6;aa=math.pi/6
    p=c.beginPath();p.moveTo(x2,y2)
    p.lineTo(x2-al*math.cos(a-aa),y2-al*math.sin(a-aa))
    p.lineTo(x2-al*math.cos(a+aa),y2-al*math.sin(a+aa));p.close()
    c.drawPath(p,fill=1);c.restoreState()

def da(c,x1,y1,x2,y2,clr=CGY):
    c.saveState();c.setStrokeColor(clr);c.setFillColor(clr);c.setLineWidth(1);c.setDash(3,3)
    c.line(x1,y1,x2,y2);c.setDash();a=math.atan2(y2-y1,x2-x1);al=5;aa=math.pi/6
    p=c.beginPath();p.moveTo(x2,y2)
    p.lineTo(x2-al*math.cos(a-aa),y2-al*math.sin(a-aa))
    p.lineTo(x2-al*math.cos(a+aa),y2-al*math.sin(a+aa));p.close()
    c.drawPath(p,fill=1);c.restoreState()

def lb(c,x,y,t,fs=6,clr=CGY):
    c.saveState();c.setFont('CF',fs);c.setFillColor(clr);c.drawCentredString(x,y,t);c.restoreState()

def mkstyles():
    s=getSampleStyleSheet()
    def a(n,**kw): s.add(ParagraphStyle(n,**kw))
    a('CT',fontName='CFB',fontSize=32,leading=44,alignment=TA_CENTER,textColor=CB,spaceAfter=20)
    a('CS',fontName='CF',fontSize=16,leading=24,alignment=TA_CENTER,textColor=CGY,spaceAfter=10)
    a('H0',fontName='CFB',fontSize=22,leading=30,textColor=CB,spaceBefore=20,spaceAfter=16)
    a('H1',fontName='CFB',fontSize=16,leading=22,textColor=C1,spaceBefore=16,spaceAfter=10)
    a('H2',fontName='CFB',fontSize=13,leading=18,textColor=C2,spaceBefore=12,spaceAfter=8)
    a('H3',fontName='CFB',fontSize=11,leading=15,textColor=C3,spaceBefore=8,spaceAfter=6)
    a('BD',fontName='CF',fontSize=10,leading=16,textColor=black,spaceBefore=3,spaceAfter=3,alignment=TA_JUSTIFY)
    a('BL',fontName='CF',fontSize=10,leading=15,textColor=black,spaceBefore=2,spaceAfter=2,leftIndent=20,bulletIndent=8)
    a('TE',fontName='CF',fontSize=12,leading=20,textColor=C1,spaceBefore=4,spaceAfter=4)
    a('TS',fontName='CF',fontSize=10,leading=16,textColor=HexColor('#555555'),spaceBefore=2,spaceAfter=2,leftIndent=20)
    return s

def mt(hdrs,rows,cw=None):
    if cw is None: cw=[480//len(hdrs)]*len(hdrs)
    d=[hdrs]+rows;t=Table(d,colWidths=cw,repeatRows=1)
    t.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),C1),('TEXTCOLOR',(0,0),(-1,0),white),('FONTNAME',(0,0),(-1,0),'CFB'),('FONTSIZE',(0,0),(-1,0),9),('FONTNAME',(0,1),(-1,-1),'CF'),('FONTSIZE',(0,1),(-1,-1),8),('ALIGN',(0,0),(-1,-1),'LEFT'),('VALIGN',(0,0),(-1,-1),'MIDDLE'),('GRID',(0,0),(-1,-1),0.5,CBD),('ROWBACKGROUNDS',(0,1),(-1,-1),[white,CLGY]),('TOPPADDING',(0,0),(-1,-1),4),('BOTTOMPADDING',(0,0),(-1,-1),4),('LEFTPADDING',(0,0),(-1,-1),6),('RIGHTPADDING',(0,0),(-1,-1),6)]))
    return t

def ib(text,bg=CLB,bc=C1):
    p=Paragraph(text,ParagraphStyle('ib',fontName='CF',fontSize=9,leading=14,textColor=HexColor('#333333')))
    t=Table([[p]],colWidths=[480])
    t.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,-1),bg),('LEFTPADDING',(0,0),(-1,-1),12),('RIGHTPADDING',(0,0),(-1,-1),12),('TOPPADDING',(0,0),(-1,-1),8),('BOTTOMPADDING',(0,0),(-1,-1),8),('LINEBEFORESTYLE',(0,0),(0,-1),'SOLID'),('LINEBEFORECOLOR',(0,0),(0,-1),bc),('LINEBEFOREWIDTH',(0,0),(0,-1),3)]))
    return t

def hf(cv,doc):
    cv.saveState();w,h=A4
    cv.setStrokeColor(C1);cv.setLineWidth(1.5);cv.line(30,h-40,w-30,h-40)
    cv.setFont('CF',8);cv.setFillColor(CGY)
    cv.drawString(30,h-36,"Flink AdaptiveScheduler 核心架构设计");cv.drawRightString(w-30,h-36,"基于 Flink 1.15.4")
    cv.setStrokeColor(CBD);cv.setLineWidth(0.5);cv.line(30,35,w-30,35)
    cv.drawCentredString(w/2,22,f"第 {doc.page} 页");cv.restoreState()

def draw_state_machine(c,w,h):
    c.setFont('CFB',11);c.setFillColor(CB);c.drawCentredString(w/2,h-15,"AdaptiveScheduler 状态机全景图")
    bw,bh=100,26;cx=w/2-bw/2;y1=h-50
    rr(c,cx,y1,bw,bh,5,CGY,text="Created",fs=9)
    y2=y1-50;rr(c,cx-20,y2,bw+40,bh,5,C2,text="WaitingForResources",fs=8);ar(c,w/2,y1,w/2,y2+bh);lb(c,w/2+40,y1-20,"startScheduling()")
    y3=y2-50;rr(c,cx-20,y3,bw+40,bh,5,C3,text="CreatingExecutionGraph",fs=8);ar(c,w/2,y2,w/2,y3+bh);lb(c,w/2+50,y2-20,"资源足够/超时")
    y4=y3-50;rr(c,cx,y4,bw,bh,5,CG,text="Executing",fs=9);ar(c,w/2,y3,w/2,y4+bh);lb(c,w/2+55,y3-20,"EG创建完成")
    rx=w-110;ry=(y3+y4)//2;rr(c,rx,ry,95,bh,5,CO,text="Restarting",fs=9);ar(c,cx+bw,y4+bh/2,rx,ry+bh/2);lb(c,(cx+bw+rx)//2,y4+bh/2+8,"可恢复故障",clr=CO)
    c.saveState();c.setStrokeColor(CO);c.setLineWidth(1);c.line(rx+95,ry+bh/2,rx+105,ry+bh/2);c.line(rx+105,ry+bh/2,rx+105,y2+bh/2);c.restoreState();ar(c,rx+105,y2+bh/2,cx+bw+20,y2+bh/2,CO)
    fx=10;fy=y4;rr(c,fx,fy,80,bh,5,CR,text="Failing",fs=9);ar(c,cx,y4+bh/2,fx+80,fy+bh/2);lb(c,(cx+fx+80)//2,y4+bh/2+8,"不可恢复",clr=CR)
    y5=y4-60;rr(c,cx,y5,bw,bh,5,CB,text="Finished",fs=9);ar(c,w/2,y4,w/2,y5+bh);lb(c,w/2+35,y4-25,"作业完成");ar(c,fx+40,fy,fx+40,y5+bh)
    rr(c,rx,y4-30,95,bh,5,CT3,text="Canceling",fs=9);da(c,cx+bw,y4+3,rx,y4-30+bh/2);ar(c,rx+47,y4-30,cx+bw,y5+bh/2,CT3)
    rr(c,fx,y3,100,bh,5,CP,text="StopWithSavepoint",fs=7);da(c,cx,y4+bh-3,fx+100,y3+bh/2)

def draw_stm(c,w,h):
    c.setFont('CFB',11);c.setFillColor(CB);c.drawCentredString(w/2,h-15,"StateTransitionManager 内部状态机")
    bw,bh=85,28;y=h-55;xs=[15,110,205,300,395]
    for i,(nm,cl) in enumerate([("Cooldown",CR),("Idling",CGY),("Stabilizing",C2),("Stabilized",CG),("Transitioning",CP)]):
        rr(c,xs[i],y,bw,bh,5,cl,text=nm,fs=8)
        if i<4: ar(c,xs[i]+bw,y+bh/2,xs[i+1],y+bh/2)
    c.setFont('CF',6);c.setFillColor(CGY)
    c.drawCentredString((xs[0]+bw+xs[1])//2,y+bh+5,"超时");c.drawCentredString((xs[1]+bw+xs[2])//2,y+bh+5,"资源变化")
    c.drawCentredString((xs[2]+bw+xs[3])//2,y+bh+5,"稳定超时");c.drawCentredString((xs[3]+bw+xs[4])//2,y+bh+5,"onTrigger")
    y2=y-25;c.saveState();c.setStrokeColor(CO);c.setLineWidth(1);c.setDash(3,3)
    c.line(xs[2]+bw/2,y,xs[2]+bw/2,y2);c.line(xs[2]+bw/2,y2,xs[1]+bw/2,y2);c.setDash();c.restoreState()
    ar(c,xs[1]+bw/2,y2,xs[1]+bw/2,y,CO)
    c.setFont('CF',6);c.setFillColor(CO);c.drawCentredString((xs[1]+xs[2])//2+bw//2,y2-8,"资源不足→回退Idling")
    y3=y-50;c.saveState();c.setStrokeColor(CR);c.setLineWidth(1);c.setDash(3,3)
    c.line(xs[4]+bw/2,y,xs[4]+bw/2,y3);c.line(xs[4]+bw/2,y3,xs[0]+bw/2,y3);c.setDash();c.restoreState()
    ar(c,xs[0]+bw/2,y3,xs[0]+bw/2,y,CR)
    c.setFont('CF',6);c.setFillColor(CR);c.drawCentredString(w/2,y3-8,"转换完成→重新Cooldown")

def draw_startup(c,w,h):
    c.setFont('CFB',11);c.setFillColor(CB);c.drawCentredString(w/2,h-15,"作业启动流程")
    bh=26;y=h-48
    rr(c,20,y,90,bh,4,CGY,text="Created",fs=8);ar(c,110,y+bh/2,150,y+bh/2);rr(c,150,y,120,bh,4,C1,text="startScheduling()",fs=7);ar(c,270,y+bh/2,310,y+bh/2);rr(c,310,y,160,bh,4,C2,text="WaitingForResources",fs=7)
    y2=y-42;rr(c,20,y2,140,bh,4,C2,text="declareDesiredResources",fs=7);ar(c,160,y2+bh/2,200,y2+bh/2);rr(c,200,y2,100,bh,4,CLB,text="SlotPool",fs=8);ar(c,300,y2+bh/2,340,y2+bh/2);rr(c,340,y2,130,bh,4,CG,text="newResourcesAvail()",fs=7)
    y3=y2-42;rr(c,20,y3,140,bh,4,C3,text="CreatingExecutionGraph",fs=7);ar(c,160,y3+bh/2,200,y3+bh/2);rr(c,200,y3,110,bh,4,CP,text="BackgroundTask",fs=8);ar(c,310,y3+bh/2,350,y3+bh/2);rr(c,350,y3,120,bh,4,CLG,text="异步创建EG",fs=8)
    y4=y3-42;rr(c,20,y4,90,bh,4,CG,text="Executing",fs=9);ar(c,110,y4+bh/2,150,y4+bh/2);rr(c,150,y4,110,bh,4,C1,text="deploy Tasks",fs=8);ar(c,260,y4+bh/2,300,y4+bh/2);rr(c,300,y4,130,bh,4,CO,text="TaskExecutors",fs=8)

def draw_rescale(c,w,h):
    c.setFont('CFB',11);c.setFillColor(CB);c.drawCentredString(w/2,h-15,"自适应扩缩容流程")
    bh=24;y=h-48
    rr(c,20,y,100,bh,4,CO,text="资源变化",fs=8);ar(c,120,y+bh/2,160,y+bh/2);rr(c,160,y,130,bh,4,C1,text="newResourcesAvail()",fs=7);ar(c,290,y+bh/2,330,y+bh/2);rr(c,330,y,140,bh,4,C2,text="StateTransitionMgr",fs=8)
    y2=y-40;rr(c,20,y2,80,bh,4,CR,text="Cooldown",fs=8);ar(c,100,y2+bh/2,130,y2+bh/2);rr(c,130,y2,80,bh,4,C2,text="Stabilizing",fs=8);ar(c,210,y2+bh/2,240,y2+bh/2);rr(c,240,y2,80,bh,4,CG,text="Stabilized",fs=8);ar(c,320,y2+bh/2,360,y2+bh/2);rr(c,360,y2,120,bh,4,CP,text="shouldRescale()",fs=7)
    y3=y2-40;rr(c,20,y3,100,bh,4,CO,text="Restarting",fs=8);ar(c,120,y3+bh/2,160,y3+bh/2);rr(c,160,y3,130,bh,4,C2,text="WaitingForResources",fs=7);ar(c,290,y3+bh/2,330,y3+bh/2);rr(c,330,y3,90,bh,4,C3,text="CreateEG",fs=8);ar(c,420,y3+bh/2,450,y3+bh/2);rr(c,450,y3,30,bh,4,CG,text="✓",fs=10)

def draw_failure(c,w,h):
    c.setFont('CFB',11);c.setFillColor(CB);c.drawCentredString(w/2,h-15,"故障恢复流程")
    bh=24;y=h-48
    rr(c,20,y,90,bh,4,CR,text="Task FAILED",fs=8);ar(c,110,y+bh/2,150,y+bh/2);rr(c,150,y,140,bh,4,C1,text="handleGlobalFailure()",fs=7);ar(c,290,y+bh/2,330,y+bh/2);rr(c,330,y,140,bh,4,CO,text="howToHandleFailure()",fs=7)
    y2=y-45;rr(c,50,y2,130,bh,4,CG,text="canRestart=true",fs=7);ar(c,180,y2+bh/2,220,y2+bh/2);rr(c,220,y2,100,bh,4,CO,text="Restarting",fs=9);ar(c,320,y2+bh/2,360,y2+bh/2);rr(c,360,y2,100,bh,4,C2,text="cancel tasks",fs=8)
    y3=y2-40;rr(c,50,y3,130,bh,4,CR,text="canRestart=false",fs=7);ar(c,180,y3+bh/2,220,y3+bh/2);rr(c,220,y3,100,bh,4,CR,text="Failing",fs=9);ar(c,320,y3+bh/2,360,y3+bh/2);rr(c,360,y3,100,bh,4,CB,text="Finished",fs=8)
    ar(c,w/2,y,115,y2+bh);ar(c,w/2,y,115,y3+bh)

def draw_slot(c,w,h):
    c.setFont('CFB',11);c.setFillColor(CB);c.drawCentredString(w/2,h-15,"SlotSharingSlotAllocator 分配流程")
    bh=24;y=h-50
    rr(c,20,y,100,bh,4,C1,text="Free Slots",fs=8);rr(c,140,y,100,bh,4,C2,text="JobInformation",fs=8);rr(c,260,y,120,bh,4,CG,text="Previous Allocs",fs=7)
    y2=y-40;ar(c,70,y,70,y2+bh);ar(c,190,y,190,y2+bh);ar(c,320,y,320,y2+bh)
    rr(c,30,y2,380,bh,4,CO,text="Step1: SlotSharingResolver → 划分SlotSharingGroup",fs=8)
    y3=y2-38;ar(c,220,y2,220,y3+bh);rr(c,30,y3,380,bh,4,CP,text="Step2: SlotMatchingResolver → 匹配Slot与Group",fs=8)
    y4=y3-38;rr(c,20,y4,120,bh,4,CLB,text="Simple",fs=8);rr(c,160,y4,120,bh,4,CLG,text="SlotsBalanced",fs=8);rr(c,310,y4,130,bh,4,CLO,text="TasksBalanced",fs=8)
    ar(c,80,y3,80,y4+bh);ar(c,220,y3,220,y4+bh);ar(c,375,y3,375,y4+bh)
    y5=y4-38;ar(c,220,y4,220,y5+bh);rr(c,30,y5,380,bh,4,CG,text="Step3: SlotAssigner → 最终分配(Default/StateLocality)",fs=8)

def draw_ctx(c,w,h):
    c.setFont('CFB',11);c.setFillColor(CB);c.drawCentredString(w/2,h-15,"Context 模式设计")
    bh=28;y=h-55;rr(c,w/2-75,y,150,36,5,CB,text="AdaptiveScheduler\n(实现所有Context)",fs=8)
    y2=y-60
    for nm,cl,cx in [("Created.Ctx",CGY,15),("WaitFor.Ctx",C2,105),("Executing.Ctx",CG,220),("Restarting.Ctx",CO,330),("Canceling.Ctx",CT3,420)]:
        rr(c,cx,y2,80,32,4,cl,text=nm,fs=6);ar(c,w/2,y,cx+40,y2+32)
    y3=y2-50
    for nm,cl,cx in [("Created",CGY,15),("WaitFor\nResources",C2,105),("Executing",CG,220),("Restarting",CO,330),("Canceling",CT3,420)]:
        rr(c,cx,y3,80,32,4,cl,text=nm,fs=7);ar(c,cx+40,y2,cx+40,y3+32)
    c.setFont('CF',6);c.setFillColor(CGY);c.drawString(15,y3-14,"每个State通过Context接口回调Scheduler，实现解耦。Factory(内嵌工厂)负责创建状态实例。")

def draw_timeline(c,w,h):
    c.setFont('CFB',11);c.setFillColor(CB);c.drawCentredString(w/2,h-15,"Rescale Timeline 架构 (FLIP-495)")
    bh=28;y=h-55;rr(c,w/2-75,y,150,bh,5,CB,text="RescaleTimeline",fs=9)
    y2=y-45;rr(c,w/2-85,y2,170,bh,5,C1,text="DefaultRescaleTimeline",fs=9);ar(c,w/2,y,w/2,y2+bh)
    y3=y2-50
    for nm,cl,cx in [("Rescale",CG,20),("RescalesSummary",CO,140),("SchedulerState\nSpan",CP,260),("VertexParallel\nRescale",C2,380)]:
        rr(c,cx,y3,100,32,4,cl,text=nm,fs=7);ar(c,w/2,y2,cx+50,y3+32)
    y4=y3-42
    for nm,cl,cx in [("RescaleIdInfo",CLB,20),("TriggerCause",CLG,115),("TerminalState",CLO,210),("TerminatedReason",CLP,305),("Durable",CLY,410)]:
        rr(c,cx,y4,80,24,3,cl,text=nm,fs=7);da(c,70,y3,cx+40,y4+24)

def build():
    register_fonts();st=mkstyles()
    out=os.path.join(os.path.dirname(os.path.abspath(__file__)),"Flink_AdaptiveScheduler_架构设计文档.pdf")
    doc=SimpleDocTemplate(out,pagesize=A4,topMargin=50,bottomMargin=50,leftMargin=55,rightMargin=55)
    S=[]
    # 封面
    S+=[Spacer(1,80),Paragraph("Apache Flink AdaptiveScheduler",st['CT']),Paragraph("核心架构设计文档",st['CT']),Spacer(1,30)]
    S+=[Paragraph("—— 基于 Flink 1.15.4 源码深度分析 ——",st['CS']),Spacer(1,40)]
    ci=[["模块","flink-runtime / scheduler / adaptive"],["版本","release-1.15.4"],["文件数","64个Java源文件 (主目录28 + allocator 24 + timeline 12)"],["核心设计","状态机模式 + 声明式资源管理 + 自适应并行度"],["关键特性","FLIP-160: 资源不足时自动降级启动作业"]]
    t=Table(ci,colWidths=[80,380]);t.setStyle(TableStyle([('FONTNAME',(0,0),(0,-1),'CFB'),('FONTNAME',(1,0),(1,-1),'CF'),('FONTSIZE',(0,0),(-1,-1),11),('TEXTCOLOR',(0,0),(0,-1),C1),('TOPPADDING',(0,0),(-1,-1),6),('BOTTOMPADDING',(0,0),(-1,-1),6),('LINEBELOW',(0,0),(-1,-2),0.5,CBD)]))
    S+=[t,PageBreak()]
    # 目录
    S+=[Paragraph("目  录",st['H0']),Spacer(1,10)]
    toc=[("第一章","AdaptiveScheduler 整体架构概览"),("  1.1","模块组成与文件结构"),("  1.2","核心设计思想与 FLIP-160"),("  1.3","与 DefaultScheduler 的对比"),("第二章","状态机模式设计"),("  2.1","核心状态机全景图"),("  2.2","State 接口与类型安全分发"),("  2.3","Context 模式设计"),("  2.4","StateWith/WithoutExecutionGraph 抽象层"),("第三章","状态转换管理器"),("  3.1","五阶段状态机"),("  3.2","资源稳定性判定与防抖"),("第四章","作业生命周期核心流程"),("  4.1","作业启动流程"),("  4.2","自适应扩缩容流程"),("  4.3","故障恢复流程"),("第五章","Slot 分配子系统"),("  5.1","SlotSharingSlotAllocator 分配流程"),("  5.2","三种 SlotMatchingResolver 策略"),("第六章","Rescale Timeline 子系统"),("  6.1","FLIP-495 架构"),("  6.2","核心数据模型"),("第七章","设计模式总结与关键类说明"),("  7.1","设计模式清单"),("  7.2","关键类职责"),("  7.3","核心方法调用链")]
    for n,ti in toc:
        S.append(Paragraph(f"<b>{n}  {ti}</b>" if n.startswith("第") else f"{n}  {ti}",st['TE'] if n.startswith("第") else st['TS']))
    S.append(PageBreak())

    # 第一章
    S+=[Paragraph("第一章  AdaptiveScheduler 整体架构概览",st['H0']),Spacer(1,8)]
    S+=[Paragraph("1.1  模块组成与文件结构",st['H1'])]
    S+=[Paragraph("AdaptiveScheduler 位于 flink-runtime 的 org.apache.flink.runtime.scheduler.adaptive 包中，是 Flink 针对弹性资源环境设计的自适应调度器，共 64 个 Java 源文件：",st['BD']),Spacer(1,6)]
    S.append(mt(["目录","文件数","核心职责"],[["adaptive/ (主目录)","28","状态机核心：调度器、各状态类、状态转换管理"],["adaptive/allocator/","24","Slot 分配子系统：分配器、匹配器、共享 Slot"],["adaptive/timeline/","12","扩缩容时间线：记录和统计 Rescale 事件"]],cw=[130,50,300]))
    S+=[Spacer(1,8),Paragraph("1.2  核心设计思想与 FLIP-160",st['H1'])]
    S+=[Paragraph("AdaptiveScheduler 实现了 FLIP-160 的自适应调度器理念。与 DefaultScheduler 要求资源完全满足后才启动不同，AdaptiveScheduler 采用声明式资源管理，能在资源不足时自动降低并行度启动，并在资源变化时动态扩缩容：",st['BD']),Spacer(1,4)]
    for s in ["<b>声明式资源管理</b>：通过 DeclarativeSlotPool 声明资源需求，而非命令式申请 Slot","<b>自适应并行度</b>：资源不足时自动计算可用最大并行度，降级启动","<b>弹性扩缩容</b>：运行中资源变化时通过 Rescale 重建 ExecutionGraph","<b>稳定性判定</b>：StateTransitionManager 多阶段确保资源稳定后才扩缩容"]:
        S.append(Paragraph(f"• {s}",st['BL']))
    S+=[Spacer(1,8)]
    S.append(ib("<b>FLIP-160 核心理念：</b>传统调度器要求所有 Slot 就位才启动。AdaptiveScheduler 先声明资源需求，然后根据实际可用 Slot 自动计算并行度。这使 Flink 在 K8s/YARN 弹性环境中不会因个别 Slot 未就绪而阻塞整个作业。"))
    S+=[Spacer(1,8),Paragraph("1.3  与 DefaultScheduler 的对比",st['H1'])]
    S.append(mt(["特性","DefaultScheduler","AdaptiveScheduler"],[["资源模型","命令式(逐个申请Slot)","声明式(声明总需求)"],["并行度","固定(配置时确定)","自适应(运行时调整)"],["扩缩容","不支持","自动(Rescale机制)"],["ExecutionGraph","启动前一次性创建","可能多次重建"],["故障恢复","Region级别","全局级别(整个作业重启)"],["适用场景","稳定资源环境","弹性资源环境(K8s/YARN)"]],cw=[100,190,190]))
    S.append(PageBreak())

    # 第二章
    S+=[Paragraph("第二章  状态机模式设计",st['H0']),Spacer(1,8)]
    S+=[Paragraph("2.1  核心状态机全景图",st['H1'])]
    S+=[Paragraph("AdaptiveScheduler 的核心是一个精心设计的有限状态机(FSM)。每个状态封装该阶段的所有行为逻辑，将复杂调度逻辑分解到独立状态类中：",st['BD']),Spacer(1,6)]
    S.append(FC(480,330,draw_state_machine))
    S+=[Spacer(1,8)]
    S.append(mt(["状态","继承","核心职责"],[["Created","WithoutEG","初始状态，等待startScheduling()"],["WaitingForResources","WithoutEG","声明资源需求，等待Slot到达"],["CreatingExecutionGraph","WithoutEG","异步创建ExecutionGraph(BackgroundTask)"],["Executing","WithEG","部署Task，处理Checkpoint，监听资源变化"],["Restarting","WithEG","取消Task，等待完成后重新WaitingForResources"],["Failing","WithEG","不可恢复故障，cancel后→Finished(FAILED)"],["Canceling","WithEG","用户取消，等待Task取消完成"],["Finished","State","终态：完成/失败/取消"],["StopWithSavepoint","WithEG","触发Savepoint后停止作业"]],cw=[110,70,300]))

    S+=[Spacer(1,10),Paragraph("2.2  State 接口与类型安全分发",st['H1'])]
    S+=[Paragraph("State 接口(198行)定义了类型安全的方法分发机制。RPC调用到达时，通过 tryRun/tryCall/as 三个泛型方法实现安全的状态感知操作：",st['BD']),Spacer(1,4)]
    S.append(mt(["方法","签名","作用"],[["tryRun()","tryRun(Class&lt;T&gt;, Consumer&lt;T&gt;, String)","当前状态是T类型则执行Consumer"],["tryCall()","tryCall(Class&lt;T&gt;, FunctionWithException)","当前状态是T类型则执行Function返回结果"],["as()","as(Class&lt;T&gt;)","安全类型转换，返回Optional&lt;T&gt;"]],cw=[60,220,200]))
    S+=[Spacer(1,6)]
    S.append(ib("<b>设计意义：</b>例如 triggerSavepoint() 只在 Executing 状态有效。通过 tryCall(Executing.class, ...) 模式，非 Executing 状态会安全抛出异常而非 ClassCastException，避免大量 if-else 状态检查。"))

    S+=[Spacer(1,10),Paragraph("2.3  Context 模式设计",st['H1']),Spacer(1,6)]
    S.append(FC(480,220,draw_ctx))
    S+=[Spacer(1,8)]
    for s in ["<b>解耦</b>：State 不直接依赖 AdaptiveScheduler，只依赖抽象 Context 接口","<b>ISP</b>：每个 Context 只含该状态需要的方法，符合接口隔离原则","<b>可测试</b>：Mock Context 即可测试，无需完整 Scheduler","<b>Factory</b>：每个 State 内嵌 Factory 类，通过 Context 注入创建实例"]:
        S.append(Paragraph(f"• {s}",st['BL']))

    S+=[Spacer(1,8),Paragraph("2.4  StateWithExecutionGraph / StateWithoutExecutionGraph",st['H1'])]
    S.append(mt(["基类","子类","特点"],[["StateWithoutExecutionGraph\n(109行)","Created, WaitingForResources,\nCreatingExecutionGraph","不持有EG；对运行时操作返回默认值"],["StateWithExecutionGraph\n(492行)","Executing, Restarting, Failing,\nCanceling, StopWithSavepoint","持有EG；实现Task操作转发、\nCheckpoint管理、Archive构建"]],cw=[150,130,200]))
    S+=[Spacer(1,6)]
    S.append(ib("<b>StateWithExecutionGraph(492行)核心功能：</b>updateTaskExecutionState()处理Task状态更新、triggerSavepoint()委托CheckpointCoordinator、goToFinished()构建ArchivedExecutionGraph转入终态。所有持有EG的状态共享这些行为。"))
    S.append(PageBreak())

    # 第三章
    S+=[Paragraph("第三章  状态转换管理器",st['H0']),Spacer(1,8)]
    S+=[Paragraph("3.1  DefaultStateTransitionManager 五阶段状态机",st['H1'])]
    S+=[Paragraph("StateTransitionManager(428行)是一个精巧的子状态机，管理资源变化到实际扩缩容之间的时序控制，解决资源频繁波动时避免不必要的EG重建：",st['BD']),Spacer(1,6)]
    S.append(FC(480,170,draw_stm))
    S+=[Spacer(1,8)]
    S.append(mt(["阶段","触发","超时行为","逻辑"],[["Cooldown","转换完成","→Idling","防抖冷却期"],["Idling","Cooldown超时","无","空闲等待资源变化"],["Stabilizing","资源变化","→Stabilized","等待稳定，新变化重置计时器"],["Stabilized","稳定超时","无","通知Scheduler执行转换"],["Transitioning","onTrigger()","→Cooldown","执行实际扩缩容"]],cw=[70,80,80,250]))
    S+=[Spacer(1,6)]
    S.append(ib("<b>防抖效果：</b>K8s环境中Pod扩缩导致Slot频繁变化。Cooldown+Stabilizing两阶段确保资源稳定后才真正Rescale。典型配置：cooldown=10s, stabilization=10s。",CLG,CG))
    S.append(PageBreak())

    # 第四章
    S+=[Paragraph("第四章  作业生命周期核心流程",st['H0']),Spacer(1,8)]
    S+=[Paragraph("4.1  作业启动流程",st['H1'])]
    S+=[Paragraph("作业启动遵循 Created → WaitingForResources → CreatingExecutionGraph → Executing 的状态链：",st['BD']),Spacer(1,6)]
    S.append(FC(480,210,draw_startup))
    S+=[Spacer(1,8)]
    for s in ["<b>startScheduling()</b> — Created 转入 WaitingForResources，声明资源需求","<b>newResourcesAvailable()</b> — 检查可用Slot，达到最小要求则创建EG","<b>超时机制</b> — resourceWaitingTimeout到期，以当前可用资源强制启动","<b>BackgroundTask</b> — 异步创建EG+恢复Checkpoint，支持abort安全取消","<b>Executing入口</b> — deploy所有Task，启动CheckpointScheduler"]:
        S.append(Paragraph(f"• {s}",st['BL']))
    S+=[Spacer(1,6)]
    S.append(ib("<b>BackgroundTask设计：</b>支持链式调用(andThen)。状态转换时abort()安全取消后续操作，避免竞态条件。"))

    S+=[Spacer(1,10),Paragraph("4.2  自适应扩缩容流程",st['H1'])]
    S+=[Paragraph("Executing 状态收到资源变化时，通过 StateTransitionManager 判断是否需要扩缩容：",st['BD']),Spacer(1,6)]
    S.append(FC(480,190,draw_rescale))
    S+=[Spacer(1,8)]
    for s in ["<b>Step 1</b>：SlotAllocator.determineParallelism() — 计算新并行度","<b>Step 2</b>：比较新旧 VertexParallelism — 并行度变化则 rescale","<b>Step 3</b>：检查 SlotsUtilization — 利用率可改善也触发 rescale","<b>Step 4</b>：Executing → Restarting → WaitingForResources → CreatingEG → Executing"]:
        S.append(Paragraph(f"• {s}",st['BL']))

    S+=[Spacer(1,10),Paragraph("4.3  故障恢复流程",st['H1'])]
    S+=[Paragraph("AdaptiveScheduler 采用全局故障恢复，以整个作业为恢复粒度：",st['BD']),Spacer(1,6)]
    S.append(FC(480,200,draw_failure))
    S+=[Spacer(1,8)]
    for s in ["<b>handleGlobalFailure()</b> — 入口，接收Throwable","<b>howToHandleFailure()</b> — 咨询RestartBackoffTimeStrategy","<b>可恢复</b>：→ Restarting(cancel tasks) → WaitingForResources","<b>不可恢复</b>：→ Failing(cancel tasks) → Finished(FAILED)","<b>Restarting细节</b>：cancel所有Task，终态后根据backoffTime延迟重启"]:
        S.append(Paragraph(f"• {s}",st['BL']))
    S.append(PageBreak())

    # 第五章
    S+=[Paragraph("第五章  Slot 分配子系统 (allocator)",st['H0']),Spacer(1,8)]
    S+=[Paragraph("5.1  SlotSharingSlotAllocator 分配流程",st['H1'])]
    S+=[Paragraph("allocator 子目录24个文件，核心 SlotSharingSlotAllocator(458行) 实现三阶段 Slot 分配：",st['BD']),Spacer(1,6)]
    S.append(FC(480,250,draw_slot))
    S+=[Spacer(1,8)]
    S+=[Paragraph("5.2  三种 SlotMatchingResolver 策略",st['H1'])]
    S.append(mt(["策略","实现类","算法","适用场景"],[["Simple","SimpleSlotMatchingResolver","顺序FIFO分配","简单快速"],["SlotsBalanced","SlotsBalancedSlotMR","均衡分配Slot到不同TE","分散负载"],["TasksBalanced","TasksBalancedSlotMR","均衡Task到不同TE","Task均匀分布"]],cw=[70,130,160,120]))
    S+=[Spacer(1,6)]
    S.append(ib("<b>allocator 三层策略：</b>SlotSharingResolver(解析Group需求) → SlotMatchingResolver(匹配Slot到Group) → SlotAssigner(分配Task到Slot)。三层分离使每层可独立替换测试。StateLocalitySlotAssigner 通过评分机制最大化状态本地性。",CLG,CG))
    S.append(PageBreak())

    # 第六章
    S+=[Paragraph("第六章  Rescale Timeline 子系统",st['H0']),Spacer(1,8)]
    S+=[Paragraph("6.1  FLIP-495 架构",st['H1'])]
    S+=[Paragraph("timeline 子目录12个文件，实现FLIP-495的扩缩容时间线记录，用于监控和调试：",st['BD']),Spacer(1,6)]
    S.append(FC(480,230,draw_timeline))
    S+=[Spacer(1,8),Paragraph("6.2  核心数据模型",st['H1'])]
    S.append(mt(["类名","行数","职责"],[["RescaleTimeline","107","顶层接口"],["DefaultRescaleTimeline","148","默认实现，维护Rescale列表"],["Rescale","468","核心类：记录单次扩缩容完整生命周期"],["RescalesSummary","137","统计摘要：聚合耗时统计"],["SchedulerStateSpan","124","调度器状态时间跨度"],["VertexParallelismRescale","161","顶点并行度变化记录"],["SlotSharingGroupRescale","160","Slot共享组变化记录"],["TriggerCause","38","触发原因：SCALING_UP/DOWN/FORCED"],["TerminalState","48","终态：DONE/FAILED/DISCARDED"]],cw=[130,40,310]))
    S.append(PageBreak())

    # 第七章
    S+=[Paragraph("第七章  设计模式总结与关键类说明",st['H0']),Spacer(1,8)]
    S+=[Paragraph("7.1  设计模式清单",st['H1'])]
    S.append(mt(["模式","应用位置","实现"],[["State Pattern","AdaptiveScheduler核心","9个状态类封装不同阶段行为"],["Factory Pattern","每个State类","内嵌Factory类注入Context创建实例"],["Strategy Pattern","allocator子系统","3种MatchingResolver+2种Assigner"],["Context Pattern","State-Scheduler解耦","每状态定义Context，Scheduler统一实现"],["Template Method","StateWithExecutionGraph","基类定义流程，子类实现具体行为"],["Observer Pattern","ResourceListener","WaitingForResources/Executing监听资源变化"],["Adapter Pattern","ForwardEdgesAdapter","适配ExecutionTopology为正向边查询"]],cw=[90,120,270]))

    S+=[Spacer(1,10),Paragraph("7.2  关键类职责说明",st['H1'])]
    S.append(mt(["类名","行数","核心职责"],[["AdaptiveScheduler","1792","核心调度器，实现SchedulerNG和所有Context"],["SlotSharingSlotAllocator","458","Slot分配核心，组合Resolver+Assigner"],["StateWithExecutionGraph","492","持有EG的状态基类，共享Task和CP管理"],["DefaultStateTransitionManager","428","五阶段子状态机，资源稳定性判定"],["Executing","389","核心执行状态，deploy/CP/资源监听"],["StopWithSavepoint","363","协调SP触发、完成和停止"],["Rescale","468","扩缩容完整生命周期记录"],["StateLocalitySlotAssigner","216","基于状态本地性优化的分配器"]],cw=[140,40,300]))

    S+=[Spacer(1,10),Paragraph("7.3  核心方法调用链",st['H1'])]
    S+=[Paragraph("以下是 AdaptiveScheduler 最重要的调用链，覆盖作业完整生命周期：",st['BD']),Spacer(1,4)]
    S+=[Paragraph("调用链 1：作业启动",st['H3'])]
    S.append(ib("AdaptiveScheduler.startScheduling() → Created.onLeave() → goToWaitingForResources() → WaitingForResources.onNewResourcesAvailable() → hasDesiredResources()/hasEnoughResources() → goToCreatingExecutionGraph() → BackgroundTask.run() → handleExecutionGraphCreation() → goToExecuting() → Executing.onEnter() → deploy all Tasks"))
    S+=[Spacer(1,6),Paragraph("调用链 2：自适应扩缩容",st['H3'])]
    S.append(ib("Executing.newResourcesAvailable() → StateTransitionManager.onChange() → Stabilizing → Stabilized → onSwitchToState() → shouldRescale() → SlotAllocator.determineParallelism() → goToRestarting() → Restarting.cancel() → onGloballyTerminalState() → goToWaitingForResources() → [新并行度创建EG]"))
    S+=[Spacer(1,6),Paragraph("调用链 3：故障恢复",st['H3'])]
    S.append(ib("Executing.handleGlobalFailure() → howToHandleFailure() → RestartBackoffTimeStrategy.canRestart() → [true] goToRestarting(backoffTime) → Restarting → WaitingForResources → [false] goToFailing() → Failing.cancel() → onGloballyTerminalState() → goToFinished(FAILED)"))
    S+=[Spacer(1,6),Paragraph("调用链 4：StopWithSavepoint",st['H3'])]
    S.append(ib("Executing.stopWithSavepoint() → goToStopWithSavepoint() → StopWithSavepoint.onEnter() → checkpointCoordinator.triggerSavepoint() → handleSavepointCompletion() → 等待Tasks完成 → goToFinished(savepointPath)"))

    doc.build(S,onFirstPage=hf,onLaterPages=hf)
    print(f"\n✅ PDF 已生成: {out}")

if __name__=='__main__':
    build()
