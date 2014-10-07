package com.quantcomponents.algo.special;

import java.util.Map;
import java.util.Properties;

import com.quantcomponents.algo.ITradingAgent;
import com.quantcomponents.algo.ITradingAgentFactory;
import com.quantcomponents.series.jdbc.IAggregatedTickDao;
import com.quantcomponents.series.jdbc.derby.AggregatedTickDao;

/**
 * @author Yura Seleznyov
 */
public class SpecialTradingAgentFactory implements ITradingAgentFactory {

	private static String aggregatedTicksDb;
	private static int amount;
	private static int par1;
	private static int par2;
	private static int par3;
	private static int par4;
	public static final String BID_ASKS = "bids-asks"; 
	//public static final String TRADES = "trades";
	public static final String DATABASE_NAME_IB = "stockDatabase-ib";
	private static volatile IAggregatedTickDao tickDao; 
	
	public static IAggregatedTickDao getTickDao() {
		return tickDao;
	}
	
	@Override
	public String[] getInputSeriesNames() {
		//return new String[]{BID_ASKS,TRADES};
		return new String[]{BID_ASKS};
	}

	@Override
	public String[] getConfigurationKeys() {
		return new String[] {SpecialTradingAgent.AGGREGATED_TICKS_DB,
				//SpecialTradingAgent.BAR_SIZE,
				SpecialTradingAgent.POSITION_SIZE,
				SpecialTradingAgent.PARAMETER_1, 
				SpecialTradingAgent.PARAMETER_2, 
				SpecialTradingAgent.PARAMETER_3,
				SpecialTradingAgent.PARAMETER_4 };
	}

	@Override
	public boolean isConfigurationValid(Properties configuration,
			Map<String, String> messages) {

		aggregatedTicksDb = configuration.getProperty(SpecialTradingAgent.AGGREGATED_TICKS_DB);
		String amountStr = configuration.getProperty(SpecialTradingAgent.POSITION_SIZE);
		String par1Str = configuration.getProperty(SpecialTradingAgent.PARAMETER_1);
		String par2Str = configuration.getProperty(SpecialTradingAgent.PARAMETER_2);
		String par3Str = configuration.getProperty(SpecialTradingAgent.PARAMETER_3);
		String par4Str = configuration.getProperty(SpecialTradingAgent.PARAMETER_4);
		
		try{
			amount = Integer.decode(amountStr);
		}catch(NumberFormatException e){
			messages.put(SpecialTradingAgent.POSITION_SIZE, "Wrong number format");
		}
		try{
			par1 = Integer.decode(par1Str);
		}catch(NumberFormatException e){
			messages.put(SpecialTradingAgent.PARAMETER_1, "Wrong number format");
		}
		try{
			par2 = Integer.decode(par2Str);
		}catch(NumberFormatException e){
			messages.put(SpecialTradingAgent.PARAMETER_2, "Wrong number format");
		}
		try{
			par3 = Integer.decode(par3Str);
		}catch(NumberFormatException e){
			messages.put(SpecialTradingAgent.PARAMETER_3, "Wrong number format");
		}
		try{
			par4 = Integer.decode(par4Str);
		}catch(NumberFormatException e){
			messages.put(SpecialTradingAgent.PARAMETER_4, "Wrong number format");
		}
		
		if (messages.size() > 0) {
			return false;
		} else {
			return true;
		}
	}

	@Override
	public ITradingAgent createProcessor(Properties configuration) {
		AggregatedTickDao dao = new AggregatedTickDao(aggregatedTicksDb);
		return new SpecialTradingAgent(amount, par1, par2, par3, par4, dao);
	}

}
