/**
 * Smart AC by RZKY Studio — virtual PLC telemetry, Cloudflare Workers + Durable Objects.
 * GitHub Pages: Dashboard / HMI / Remote; controls stay behind Supabase Auth + SQL RPC.
 * This public WebSocket is read-only. No actual hardware is controlled.
 * The 1-second PLC scan is active while at least one browser WebSocket is connected.
 */
const ALLOWED_ORIGINS = new Set([
  'https://dashboard.penyelamatharimu.my.id',
  'https://ac.penyelamatharimu.my.id',
  'https://remote.penyelamatharimu.my.id',
  'https://rizkyyrz.github.io',
]);
const FAN_LEVEL = { LOW: 1, MED: 2, HIGH: 3, AUTO: 2 };
const round = (n, digits=2) => Number(Number(n).toFixed(digits));
function json(data, status=200) {
  return new Response(JSON.stringify(data), {
    status, headers: {'Content-Type':'application/json; charset=utf-8', 'Cache-Control':'no-store',
      'Access-Control-Allow-Origin':'*'}
  });
}

export default {
  async fetch(request, env) {
    const url=new URL(request.url);
    if (url.pathname === '/') {
      return json({name:'Smart AC by RZKY Studio',service:'virtual-plc',endpoints:['/health','/ws'],
        control:'Supabase Auth + issue_ac_command RPC',virtual:true});
    }
    if (!['/ws','/health'].includes(url.pathname)) return json({error:'Not found'},404);
    if (url.pathname==='/ws') {
      if ((request.headers.get('Upgrade')||'').toLowerCase()!=='websocket') {
        return json({error:'WebSocket upgrade required'},426);
      }
      const origin=request.headers.get('Origin');
      if (origin && !ALLOWED_ORIGINS.has(origin)) return json({error:'Origin not allowed'},403);
    }
    const id=env.PLC.idFromName('smart-ac-campus-global');
    const stub=env.PLC.get(id);
    return stub.fetch(request);
  }
};

export class PlcCampus {
  constructor(ctx,env) {
    this.ctx=ctx;
    this.env=env;
    this.units=new Map();
    this.temperatureOutside=31;
    this.connected=false;
    this.lastPoll=null;
    this.lastError=null;
    this.lastSnapshotAt=null;
    this.lastWeatherAt=0;
    this.initialized=false;
    this.starting=null;
    this.timers=[];
    this.lastSave=0;
    this.latestPacket=null;
  }

  dbConfigured(){return Boolean(this.env.SUPABASE_URL && this.env.SUPABASE_SECRET_KEY);}

  async dbRequest(path, opts={}) {
    if (!this.dbConfigured()) throw Error('Set SUPABASE_URL and SUPABASE_SECRET_KEY in Worker secrets');
    const endpoint=this.env.SUPABASE_URL.replace(/\/$/,'')+'/rest/v1/'+path;
    const response=await fetch(endpoint,{
      ...opts,
      headers:{'apikey':this.env.SUPABASE_SECRET_KEY,
        'Content-Type':'application/json',...(opts.headers||{})},
      signal:AbortSignal.timeout(9000),
    });
    if (!response.ok) throw Error('Supabase REST HTTP '+response.status+': '+(await response.text()).slice(0,180));
    if (response.status===204) return null;
    const text=await response.text();
    return text?JSON.parse(text):null;
  }

  applyRow(row){
    let u=this.units.get(row.id);
    if(!u){
      u={id:row.id,roomCode:row.room_code,label:row.label,roomTemp:29.2,
        compressor:false,alarmHighTemp:false,powerKw:0};
      this.units.set(row.id,u);
    }
    u.roomCode=row.room_code;
    u.label=row.label;
    u.power=Boolean(row.power_on);
    u.autoMode=row.control_mode==='AUTO';
    u.swing=Boolean(row.swing_on);
    u.setpoint=Math.max(16,Math.min(30,Number(row.target_temp)||24));
    u.fanMode=row.fan_mode||'AUTO';
    u.fanSpeed=FAN_LEVEL[u.fanMode]||2;
    u.hvacMode=row.hvac_mode||'COOL';
  }

  async pollSupabase(){
    try{
      const rows=await this.dbRequest('ac_units?select=id,room_code,label,power_on,control_mode,swing_on,target_temp,fan_mode,hvac_mode');
      const seen=new Set();
      for(const row of (rows||[])){this.applyRow(row);seen.add(row.id)}
      for(const id of this.units.keys())if(!seen.has(id))this.units.delete(id);
      this.connected=true;
      this.lastError=null;
      this.lastPoll=new Date().toISOString();
    }catch(e){
      this.connected=false;this.lastError=String(e.message||e).slice(0,200);
      console.error('Supabase sync:',this.lastError);
    }
  }

  async restoreTelemetry(){
    try{
      const rows=await this.dbRequest('plc_telemetry?select=unit_id,room_temp,compressor');
      for(const row of (rows||[])){
        const u=this.units.get(row.unit_id);
        if(u && Number.isFinite(Number(row.room_temp))){
          u.roomTemp=Number(row.room_temp);u.compressor=Boolean(row.compressor);
        }
      }
    }catch(e){console.warn('Telemetry restore:',e.message)}
  }

  async updateWeather(){
    try{
      const res=await fetch('https://api.open-meteo.com/v1/forecast?latitude=-6.1177&longitude=106.1539&current=temperature_2m&timezone=Asia%2FJakarta',
        {signal:AbortSignal.timeout(8000)});
      if(!res.ok)throw Error('Open-Meteo HTTP '+res.status);
      const data=await res.json(),value=Number(data.current?.temperature_2m);
      if(Number.isFinite(value))this.temperatureOutside=value;
    }catch(e){console.warn('Cuaca fallback:',e.message)}
    this.lastWeatherAt=Date.now();
  }

  snapshot(u){return {
    unitId:u.id,roomCode:u.roomCode,label:u.label,
    power:u.power,autoMode:u.autoMode,compressor:u.compressor,swing:u.swing,
    fanSpeed:u.fanSpeed,fanMode:u.fanMode,
    setpoint:u.setpoint,roomTemp:round(u.roomTemp),ambientTemp:round(this.temperatureOutside,1),
    powerKw:round(u.powerKw),alarmHighTemp:u.alarmHighTemp,
    registers:{'%Q0.0':u.power?1:0,'%M0.0':u.autoMode?1:0,'%Q0.1':u.compressor?1:0,
      '%Q0.2':u.swing?1:0,'%Q0.3':u.alarmHighTemp?1:0,
      '%MW10':u.setpoint,'%MW12':round(u.roomTemp),'%MW16':round(u.powerKw),'%MW20':u.fanSpeed}
  }};

  packet(){return JSON.stringify({type:'PLC_STATE',
    data:[...this.units.values()].map(u=>this.snapshot(u)),
    time:new Date().toISOString(),connectedToDatabase:this.connected,
    virtual:true,scanIntervalMs:1000});}

  broadcast(){
    const packet=this.packet();
    for(const socket of this.ctx.getWebSockets()){
      try{socket.send(packet)}catch(e){console.warn('socket send:',e.message)}
    }
  }

  scan(){
    for(const u of this.units.values()){
      if(!u.power || u.hvacMode==='FAN')u.compressor=false;
      else if(u.autoMode){
        // PLC thermostat: compressor ON > setpoint+0.4, OFF <= setpoint-0.4.
        if(u.roomTemp>u.setpoint+0.4)u.compressor=true;
        else if(u.roomTemp<=u.setpoint-0.4)u.compressor=false;
      }else u.compressor=true;
      if(u.power){
        if(u.compressor){
          const coolingRate=u.fanSpeed===3?0.35:u.fanSpeed===2?0.25:0.15;
          u.roomTemp=Math.max(16,round(u.roomTemp-coolingRate));
          u.powerKw=round(0.95+u.fanSpeed*0.22);
        }else{
          u.roomTemp=round(u.roomTemp+Math.min(0.07,Math.max(-0.07,this.temperatureOutside-u.roomTemp)));
          u.powerKw=round(0.08+u.fanSpeed*0.03);
        }
      }else{
        u.powerKw=0;
        const delta=this.temperatureOutside-u.roomTemp;
        u.roomTemp=round(u.roomTemp+Math.sign(delta)*Math.min(Math.abs(delta),delta>=0?0.12:0.05));
      }
      u.alarmHighTemp=u.roomTemp>=30.0;
    }
    this.lastSnapshotAt=new Date().toISOString();
    this.broadcast();
  }

  async persistTelemetry(){
    if(!this.connected || !this.units.size)return;
    try{
      const rows=[...this.units.values()].map(u=>({
        unit_id:u.id,room_temp:round(u.roomTemp),compressor:u.compressor,
        alarm_high_temp:u.alarmHighTemp,power_kw:round(u.powerKw),
        ambient_temp:round(this.temperatureOutside,1),updated_at:new Date().toISOString()
      }));
      await this.dbRequest('plc_telemetry?on_conflict=unit_id',{
        method:'POST',headers:{Prefer:'resolution=merge-duplicates,return=minimal'},
        body:JSON.stringify(rows)
      });
      this.lastSave=Date.now();
    }catch(e){console.error('Save telemetry:',e.message)}
  }

  async begin(){
    if(this.timers.length || this.starting)return this.starting;
    this.starting=(async()=>{
      await this.pollSupabase();
      if(!this.initialized && this.connected){await this.restoreTelemetry();this.initialized=true}
      await this.updateWeather();
      if(!this.ctx.getWebSockets().length)return;
      // Keep the 1-second scan active only while someone is viewing the simulation.
      this.timers.push(setInterval(()=>this.scan(),1000));
      this.timers.push(setInterval(()=>this.pollSupabase(),3500));
      this.timers.push(setInterval(()=>this.persistTelemetry(),30000));
      this.timers.push(setInterval(()=>this.updateWeather(),15*60*1000));
      this.broadcast();
    })().finally(()=>{this.starting=null});
    return this.starting;
  }

  async stopIfUnused(){
    if(this.ctx.getWebSockets().length)return;
    for(const timer of this.timers)clearInterval(timer);
    this.timers=[];
    await this.persistTelemetry();
  }

  async fetch(request){
    const url=new URL(request.url);
    if(url.pathname==='/health'){
      // Do not need the simulation to remain active when nobody is viewing it.
      if(!this.lastPoll)await this.pollSupabase();
      return json({status:this.connected?'ok':this.dbConfigured()?'waiting_for_supabase':'missing_secrets',
        service:'Cloudflare Workers Durable Object',unitCount:this.units.size,
        connectedToDatabase:this.connected,lastPoll:this.lastPoll,
        activeWebSockets:this.ctx.getWebSockets().length,virtual:true,
        scanIntervalMs:1000,scanActive:this.timers.length>0,
        error:this.lastError});
    }
    if(url.pathname==='/ws'){
      const pair=new WebSocketPair();
      const [client,server]=Object.values(pair);
      this.ctx.acceptWebSocket(server);
      server.send(this.packet());
      this.begin().catch(e=>console.error('PLC start:',e.message));
      return new Response(null,{status:101,webSocket:client});
    }
    return json({error:'not found'},404);
  }
  webSocketMessage(socket,message){
    if(String(message).length<256 && String(message).includes('PING')){
      socket.send(JSON.stringify({type:'PONG'}));
    }
    if(!this.timers.length)this.begin().catch(console.error);
  }
  async webSocketClose(){await this.stopIfUnused()}
  async webSocketError(){await this.stopIfUnused()}
}
