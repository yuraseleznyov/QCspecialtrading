package com.quantcomponents.algo.special;

import java.util.Date;
import java.util.Map;

import com.quantcomponents.algo.IOrderReceiver;
import com.quantcomponents.algo.IPosition;
import com.quantcomponents.algo.ITradingAgent;
import com.quantcomponents.algo.OrderBean;
import com.quantcomponents.core.model.DataType;
import com.quantcomponents.core.model.IContract;
import com.quantcomponents.core.model.ISeries;
import com.quantcomponents.core.model.ISeriesAugmentable;
import com.quantcomponents.core.model.ISeriesListener;
import com.quantcomponents.core.model.ISeriesPoint;
import com.quantcomponents.core.model.OrderSide;
import com.quantcomponents.core.model.OrderType;
import com.quantcomponents.marketdata.AggregatedBar;
import com.quantcomponents.marketdata.AggregatedTick;
import com.quantcomponents.marketdata.ITickPoint;
import com.quantcomponents.marketdata.OHLCVirtualTimeSeries;
import com.quantcomponents.marketdata.TicksAndBarsAggregatedSpecial;
import com.quantcomponents.series.jdbc.IAggregatedTickDao;

/**
 * @author Yura Seleznyov
 */
public class SpecialTradingAgent implements ITradingAgent,
		ISeriesListener<Date, Double> {

	public static final String AGGREGATED_TICKS_DB = "aggregated_ticks_db_name";
	//public static final String BAR_SIZE = "bar_size";
	public static final String POSITION_SIZE = "positionSize";
	public static final String PARAMETER_1 = "par1";
	public static final String PARAMETER_2 = "par2";
	public static final String PARAMETER_3 = "par3";
	public static final String PARAMETER_4 = "par4";
	
	private OHLCVirtualTimeSeries bidAskTimeSeries;
	//private OHLCVirtualTimeSeries tradeTimeSeries;
	private IContract contract;
	private TicksAndBarsAggregatedSpecial aggregator;
	private IAggregatedTickDao dao;
	
	private IOrderReceiver orderReceiver;
	
	private volatile RunningStatus runningStatus = RunningStatus.NEW;
	
	private int amount;
	private int par1;
	private int par2;
	private int par3;
	private int par4;
	
	public SpecialTradingAgent(int amount, int par1, int par2, int par3, int par4, IAggregatedTickDao tickDao) {
		this.amount = amount;
		this.par1 = par1;
		this.par2 = par2;
		this.par3 = par3;
		this.par4 = par4;
		aggregator = new TicksAndBarsAggregatedSpecial();
		this.dao = tickDao; 
	}
	
	@Override
	public void wire(
			Map<String, ? extends ISeries<Date, Double, ? extends ISeriesPoint<Date, Double>>> input,
			ISeriesAugmentable<Date, Double, ISeriesPoint<Date, Double>> output) {
		//Check if a proper time series object type is passed
		if (!(input.get(SpecialTradingAgentFactory.BID_ASKS) instanceof OHLCVirtualTimeSeries)
			  /*|| !(input.get(SpecialTradingAgentFactory.TRADES) instanceof OHLCVirtualTimeSeries)*/) {
			throw new IllegalArgumentException("Only '" + OHLCVirtualTimeSeries.class.getName() + "' instances can be passed as input series");
		}
		
		//Check if data types passed in time series are correct
		bidAskTimeSeries = (OHLCVirtualTimeSeries) input.get(SpecialTradingAgentFactory.BID_ASKS);
		//tradeTimeSeries = (OHLCVirtualTimeSeries) input.get(SpecialTradingAgentFactory.TRADES);
		if (bidAskTimeSeries.getDataType() != DataType.BID_ASK /*|| tradeTimeSeries.getDataType() != DataType.TRADES*/){
			throw new IllegalArgumentException("Data types for time series should be '" + SpecialTradingAgentFactory.BID_ASKS + "'");
		}

		//Check if the same contract was chosen for both time series
		/*if (!bidAskTradeTimeSeries.getContract().equals(tradeTimeSeries.getContract())){
			throw new IllegalArgumentException("Contract should be the same for both data time series");
		}*/
		
		contract = bidAskTimeSeries.getContract();
	}

	@Override
	public void unwire() {aggregator.close();}
	
	@Override
	public void pause() {
		aggregator.close();
		synchronized (runningStatus) {
			if (runningStatus == RunningStatus.RUNNING) {
				runningStatus = RunningStatus.PAUSED;
			}
		}
	}

	@Override
	public void resume() {
		synchronized (runningStatus) {
			if (runningStatus == RunningStatus.PAUSED) {
				runningStatus = RunningStatus.RUNNING;
			}
		}
	}

	@Override
	public synchronized void kill() {
		aggregator.close();
		synchronized (runningStatus) {
			runningStatus = RunningStatus.TERMINATED;
		}
		
		notify();
		bidAskTimeSeries.removeTickListener(this);
		//tradeTimeSeries.removeTickListener(this);
	}

	@Override
	public RunningStatus getRunningStatus() {
		return runningStatus;
	}

	@Override
	public void run() {
		synchronized (runningStatus) {
			if (runningStatus != RunningStatus.NEW) {
				throw new IllegalStateException("Could not run from running status: " + runningStatus.name());
			}
			runningStatus = RunningStatus.RUNNING;
		}

		/*if (bidAskTradeTimeSeries.getBarSize()!=tradeTimeSeries.getBarSize()){
			System.out.print("Bar size is not equal for bidAsk series and trade series - bar siaze from bidAsk series will be used");
		}*/
		aggregator.setBarDuration(bidAskTimeSeries.getBarSize());
		bidAskTimeSeries.addTickListener(this);
		/*tradeTimeSeries.addTickListener(this);*/
	}

	@Override
	public void onOrderSubmitted(String orderId, boolean active) {}

	@Override
	public void onOrderFilled(String orderId, int filled, boolean full,
			double averagePrice) {}

	@Override
	public void onOrderCancelled(String orderId) {}

	@Override
	public void onOrderStatus(String orderId, String status) {}

	@Override
	public void onPositionUpdate(IContract contract, IPosition position) {}

	@Override
	public void setOrderReceiver(IOrderReceiver orderReceiver) {
		this.orderReceiver = orderReceiver;
	}

	@Override
	public void inputComplete() {
		kill();
	}

	@Override
	public void onItemUpdated(ISeriesPoint<Date, Double> existingItem,
			ISeriesPoint<Date, Double> updatedItem) {
		//never
	}

	@Override
	public synchronized void onItemAdded(ISeriesPoint<Date, Double> newItem) {
		try {
			ITickPoint tickPoint = (ITickPoint)newItem;
			AggregatedTick tick = aggregator.aggregateTickBuildBar(tickPoint,(int)(1000000*(tickPoint.getIndex().getTime()%1000)));
			
			if (tick == null){
				return;
			}
			dao.save(tick);
			
			AggregatedBar bar = aggregator.getLastAggregatedBar();
			if (bar==null){
				return;
			}
			
			double rt_Vol = tick.getVolume(), rt_Buy=tick.getBuy(), rt_Sell=tick.getSell(), rt_Dev = tick.getPriceDeviation()
					, prev_DEV = bar.getMaximumPriceDeviation();
			double A = par1/rt_Vol * (par2 + rt_Buy - rt_Sell) * par3/prev_DEV + Math.sin(Math.pow(par4,3) * rt_Vol);
			
			OrderSide orderSide = null;
			if (A>1 && rt_Dev<0){
				orderSide = OrderSide.BUY;
			}
			if (A<1 && rt_Dev>0){
				orderSide = OrderSide.SELL;
			}
			if (orderSide == null) return;
			
			OrderBean order = new OrderBean(contract, orderSide, OrderType.MARKET, amount, 0.0, 0.0);
			String orderId = orderReceiver.sendOrder(order);
			order.setId(orderId);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
