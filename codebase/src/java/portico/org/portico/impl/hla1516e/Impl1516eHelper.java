/*
 *   Copyright 2012 The Portico Project
 *
 *   This file is part of portico.
 *
 *   portico is free software; you can redistribute it and/or modify
 *   it under the terms of the Common Developer and Distribution License (CDDL) 
 *   as published by Sun Microsystems. For more information see the LICENSE file.
 *   
 *   Use of this software is strictly AT YOUR OWN RISK!!!
 *   If something bad happens you do not have permission to come crying to me.
 *   (that goes for your lawyer as well)
 *
 */
package org.portico.impl.hla1516e;

import hla.rti1516e.CallbackModel;
import hla.rti1516e.FederateAmbassador;
import hla.rti1516e.LogicalTime;
import hla.rti1516e.LogicalTimeFactory;
import hla.rti1516e.LogicalTimeFactoryFactory;
import hla.rti1516e.LogicalTimeInterval;
import hla.rti1516e.exceptions.CallNotAllowedFromWithinCallback;
import hla.rti1516e.exceptions.FederateNotExecutionMember;
import hla.rti1516e.exceptions.InTimeAdvancingState;
import hla.rti1516e.exceptions.InvalidLogicalTime;
import hla.rti1516e.exceptions.InvalidLookahead;
import hla.rti1516e.exceptions.NotConnected;
import hla.rti1516e.exceptions.RTIinternalError;
import hla.rti1516e.exceptions.RequestForTimeConstrainedPending;
import hla.rti1516e.exceptions.RequestForTimeRegulationPending;
import hla.rti1516e.exceptions.RestoreInProgress;
import hla.rti1516e.exceptions.SaveInProgress;
import hla.rti1516e.exceptions.SynchronizationPointLabelNotAnnounced;
import hla.rti1516e.exceptions.TimeRegulationIsNotEnabled;

import org.apache.log4j.Logger;
import org.portico.impl.HLAVersion;
import org.portico.impl.ISpecHelper;
import org.portico.impl.hla1516e.types.time.DoubleTime;
import org.portico.impl.hla1516e.types.time.DoubleTimeFactory;
import org.portico.impl.hla1516e.types.time.DoubleTimeInterval;
import org.portico.impl.hla1516e.types.time.LongTime;
import org.portico.impl.hla1516e.types.time.LongTimeFactory;
import org.portico.impl.hla1516e.types.time.LongTimeInterval;
import org.portico.lrc.LRC;
import org.portico.lrc.LRCState;
import org.portico.lrc.PorticoConstants;
import org.portico.lrc.compat.JConcurrentAccessAttempted;
import org.portico.lrc.compat.JConfigurationException;
import org.portico.lrc.compat.JEnableTimeConstrainedPending;
import org.portico.lrc.compat.JEnableTimeRegulationPending;
import org.portico.lrc.compat.JFederateNotExecutionMember;
import org.portico.lrc.compat.JInvalidFederationTime;
import org.portico.lrc.compat.JRestoreInProgress;
import org.portico.lrc.compat.JSaveInProgress;
import org.portico.lrc.compat.JSynchronizationLabelNotAnnounced;
import org.portico.lrc.compat.JTimeAdvanceAlreadyInProgress;
import org.portico.lrc.model.ObjectModel;
import org.portico.utils.messaging.MessageContext;

/**
 * This class helps provides helper methods to the {@link Rti1516eAmbassador} class and helps
 * bridge the gap between the Portico compatibility layer and the HLA 1516e interface. The basic
 * thrust of this class (like all {@link ISpecHelper} implementations) is that it provides the
 * necessary facilities to turn HLA-interface-specific code into Portico-clean, interface
 * independant code.
 * <p/>
 * To maintain the independence of Portico from any particular HLA interface specification, this is
 * one of the *ONLY* classes that is allowed to have classes from the hla.rti namespace in it. All
 * other classes should use the facilities provided by the {@link org.portico.lrc.compat} package.
 */
public class Impl1516eHelper implements ISpecHelper
{
	//----------------------------------------------------------
	//                    STATIC VARIABLES
	//----------------------------------------------------------

	//----------------------------------------------------------
	//                   INSTANCE VARIABLES
	//----------------------------------------------------------
	private LRC lrc;
	private LRCState state;
	private CallbackModel callbackModel;
	
	private FederateAmbassador fedamb;
	private LogicalTimeFactory timeFactory;
	//----------------------------------------------------------
	//                      CONSTRUCTORS
	//----------------------------------------------------------

	public Impl1516eHelper() throws RTIinternalError
	{
		try
		{
			this.lrc = new LRC( this );
			this.state = this.lrc.getState();
			this.callbackModel = CallbackModel.HLA_EVOKED;
			this.timeFactory = LogicalTimeFactoryFactory.getLogicalTimeFactory( "" );
		}
		catch( JConfigurationException jce )
		{
			throw new RTIinternalError( jce.getMessage(), jce );
		}
	}
	
	//----------------------------------------------------------
	//                    INSTANCE METHODS
	//----------------------------------------------------------
	
	////////////////////////////////////////////////////////////////////////////
	///////////////////////////// Lifecyle Methods /////////////////////////////
	////////////////////////////////////////////////////////////////////////////
	public void connected( FederateAmbassador fedamb )
	{
		setFederateAmbassador( fedamb );
	}

	public void disconnected()
	{
		setFederateAmbassador( null );
	}
	
	////////////////////////////////////////////////////////////////////////////
	////////////////////// Implementation Helper Methods ///////////////////////
	////////////////////////////////////////////////////////////////////////////

	public HLAVersion getHlaVersion()
	{
		return HLAVersion.IEEE1516e;
	}
	
	public CallbackModel getCallbackModel()
	{
		return this.callbackModel;
	}

	public void setCallbackModel( CallbackModel callbackModel )
	{
		this.callbackModel = callbackModel;
	}

	public void processMessage( MessageContext context ) throws Exception
	{
		this.lrc.getOutgoingSink().process( context );
	}
	
	public LRCState getState()
	{
		return this.state;
	}
	
	public LRC getLrc()
	{
		return this.lrc;
	}

	public ObjectModel getFOM()
	{
		return this.state.getFOM();
	}

	public LogicalTimeFactory getTimeFactory()
	{
		return this.timeFactory;
	}

	public void setTimeFactory( String timeName )
	{
		this.timeFactory = LogicalTimeFactoryFactory.getLogicalTimeFactory( timeName );
	}
	
	/**
	 * Process a single callback, waiting as long as the given timeout (in seconds) for one if
	 * there are none pendings. Return true if there are more messages waiting to be processed
	 * or false if there are none.
	 * 
	 * @param timeout The length of time to wait if there are no callbacks to process (in seconds)
	 * @return True if there are still more callbacks that can be processed, false otherwise
	 */
	public boolean evokeSingle( double timeout ) throws CallNotAllowedFromWithinCallback,
	                                                    RTIinternalError
	{
		try
		{
			return this.lrc.tickSingle( timeout );
		}
		catch( JConcurrentAccessAttempted concurrent )
		{
			throw new CallNotAllowedFromWithinCallback( concurrent.getMessage(), concurrent );
		}
		catch( Exception e )
		{
			throw new RTIinternalError( e.getMessage(), e );
		}
	}

	/**
	 * Try and process as many messages as possible. Spend up to <code>max</code> time doing so.
	 * If there are none to process, wait only as long as <code>min</code>. Return true if there
	 * are more callbacks that could be processed, false otherwise
	 *  
	 * @param min The minimum amount of time (in seconds) to wait if there are no callbacks
	 * to process
	 * @param max The maximum amount of time to process messages for (in seconds)
	 * @return True if there are still more callbacks that can be processed, false otherwise
	 */
	public boolean evokeMultiple( double min, double max ) throws CallNotAllowedFromWithinCallback,
	                                                              RTIinternalError
	{
		try
		{
			return this.lrc.tick( min, max );
		}
		catch( JConcurrentAccessAttempted concurrent )
		{
			throw new CallNotAllowedFromWithinCallback( concurrent.getMessage(), concurrent );
		}
		catch( Exception e )
		{
			throw new RTIinternalError( e.getMessage(), e );
		}
	}
	
	////////////////////////////////////////////////////////////////////////////
	/////////////////////////// Convenience Methods ////////////////////////////
	////////////////////////////////////////////////////////////////////////////
	/**
	 * Checks to make sure the federate has connected to the RTI. Throws a {@link NotConnected}
	 * exception if it is not.
	 */
	public void checkConnected() throws NotConnected
	{
		if( this.fedamb == null )
			throw new NotConnected( "Federate has not yet called connect()" );
	}

	/**
	 * Check to see if we are currently ticking (and thus not able to make an RTI callback). If
	 * we are currently ticking, a {@link hla.rti.ConcurrentAccessAttempted
	 * ConcurrentAccessAttempted} will be thrown. 
	 */
	public void checkAccess() throws CallNotAllowedFromWithinCallback
	{
		try
		{
			state.checkAccess();
		}
		catch( JConcurrentAccessAttempted ca )
		{
			throw new CallNotAllowedFromWithinCallback( ca.getMessage() );
		}
	}
	
	/**
	 * Check to see if we are advancing. If we are, throw an exception. 
	 */
	public void checkAdvancing() throws InTimeAdvancingState
	{
		try
		{
			state.checkAdvancing();
		}
		catch( JTimeAdvanceAlreadyInProgress ca )
		{
			throw new InTimeAdvancingState( ca.getMessage() );
		}
	}
	
	/**
	 * Check to see if there is a time regulation enable pending. If there is, throw an exception 
	 */
	public void checkTimeRegulation() throws RequestForTimeRegulationPending
	{
		try
		{
			state.checkTimeRegulation();
		}
		catch( JEnableTimeRegulationPending erp )
		{
			throw new RequestForTimeRegulationPending( erp.getMessage() );
		}
	}
	
	/**
	 * Check to see if there is a time constrained enable pending. If there is, throw an exception 
	 */
	public void checkTimeConstrained() throws RequestForTimeConstrainedPending
	{
		try
		{
			state.checkTimeConstrained();
		}
		catch( JEnableTimeConstrainedPending ecp )
		{
			throw new RequestForTimeConstrainedPending( ecp.getMessage() );
		}
	}
	
	/**
	 * Validate that the given time is valid for the current state (that it is equal to or greater
	 * than the current LBTS for <b>this federate</b>).
	 */
	public void checkValidTime( double time ) throws InvalidLogicalTime
	{
		try
		{
			state.checkValidTime( time );
		}
		catch( JInvalidFederationTime ift )
		{
			throw new InvalidLogicalTime( ift.getMessage() );
		}
	}
	
	public void checkSave() throws SaveInProgress
	{
		try
		{
			state.checkSave();
		}
		catch( JSaveInProgress sip )
		{
			throw new SaveInProgress( sip.getMessage() );
		}
	}
	
	public void checkRestore() throws RestoreInProgress
	{
		try
		{
			state.checkRestore();
		}
		catch( JRestoreInProgress sip )
		{
			throw new RestoreInProgress( sip.getMessage() );
		}
	}
	
	/**
	 * This method checks to see if the federate associated with this LRC is joined
	 * to a federation. If it is not, a FederateNotExecutionMember exception is thrown. 
	 */
	public void checkJoined() throws FederateNotExecutionMember
	{
		try
		{
			state.checkJoined();
		}
		catch( JFederateNotExecutionMember nem )
		{
			throw new FederateNotExecutionMember( nem.getMessage() );
		}
	}
	
	/**
	 * Checks to see if the given synchronization point label has been announced 
	 */
	public void checkSyncAnnounced( String label ) throws SynchronizationPointLabelNotAnnounced
	{
		try
		{
			state.checkSyncAnnounced( label );
		}
		catch( JSynchronizationLabelNotAnnounced na )
		{
			throw new SynchronizationPointLabelNotAnnounced( na.getMessage() );
		}
	}

	////////////////////////////////////////////////////////////////////////////
	///////////////////////////// Helper  Methods //////////////////////////////
	////////////////////////////////////////////////////////////////////////////
	public FederateAmbassador getFederateAmbassador()
	{
		return this.fedamb;
	}
	
	public void setFederateAmbassador( FederateAmbassador fedamb )
	{
		this.fedamb = fedamb;
	}

	public Logger getLrcLogger()
	{
		return this.lrc.getLrcLogger();
	}
	
	protected void reinitializeLrc()
	{
		this.lrc.reinitialize();
	}

	public double convertLookahead( LogicalTimeInterval theLookahead ) throws InvalidLookahead, RTIinternalError
	{
		if( theLookahead == null )
			throw new InvalidLookahead( "Expecting LogicalTimeInterval, found: null" );

		if (timeFactory instanceof DoubleTimeFactory)
		{
			if ( theLookahead instanceof DoubleTimeInterval )
			{
				return ((DoubleTimeInterval)theLookahead).getValue();
			}
			else
			{
				throw new InvalidLookahead( "Expecting HLAfloat64Interval, found: " + theLookahead.getClass() );
			}
		}
		else if (timeFactory instanceof LongTimeFactory)
		{
			if ( theLookahead instanceof LongTimeInterval )
			{
				return ((LongTimeInterval)theLookahead).getValue();
			}
			else
			{
				throw new InvalidLookahead( "Expecting HLAinteger64Interval, found: " + theLookahead.getClass() );
			}
		}
		else
		{
			throw new RTIinternalError( "Unexpected time factory" );
		}
	}

	public LogicalTimeInterval getLookahead() throws RTIinternalError, TimeRegulationIsNotEnabled
	{
		// make sure we are actually regulating
		// RM commented out as QueryTest.testQueryLookahead and 
		// TimeAdvanceRequestTest.testTarWithSingleRegulatingAndConstrained 
		// make requests when not joined
		//if( state.getTimeStatus().isRegulating() == false )
		//	throw new TimeRegulationIsNotEnabled( "try to access lookahead when not regulating" );

		double lookahead = getState().getLookahead();
		if (timeFactory instanceof DoubleTimeFactory)
		{
			return ( (DoubleTimeFactory) timeFactory).makeInterval( lookahead );
		}
		else if (timeFactory instanceof LongTimeFactory)
		{
			return ( (LongTimeFactory) timeFactory).makeInterval( (long)lookahead );
		}
		else
		{
			throw new RTIinternalError( "Unexpected time factory" );
		}
	}

	public double getTime( LogicalTime theTime ) throws RTIinternalError, InvalidLogicalTime
	{
		if( theTime == null )
			return PorticoConstants.NULL_TIME;

		if (timeFactory instanceof DoubleTimeFactory)
		{
			if ( theTime instanceof DoubleTime)
			{
				return ((DoubleTime)theTime).getValue();
			}
			else
			{
				throw new InvalidLogicalTime( "Expecting HLAfloat64Time, found: " + theTime.getClass() );
			}
		}
		else if (timeFactory instanceof LongTimeFactory)
		{
			if ( theTime instanceof LongTime )
			{
				return ((LongTime)theTime).getValue();
			}
			else
			{
				throw new InvalidLogicalTime( "Expecting HLAinteger64Time, found: " + theTime.getClass() );
			}
		}
		else
		{
			throw new RTIinternalError( "Unexpected time factory" );
		}
	}

	public LogicalTime getCurrentLogicalTime() throws RTIinternalError
	{
		return getLogicalTime( getState().getCurrentTime() );
	}

	public LogicalTime getLogicalTime( double time ) throws RTIinternalError
	{
		if (timeFactory instanceof DoubleTimeFactory)
		{
			return ( (DoubleTimeFactory) timeFactory).makeTime( time );
		}
		else if (timeFactory instanceof LongTimeFactory)
		{
			return ( (LongTimeFactory) timeFactory).makeTime( (long)time );
		}
		else
		{
			throw new RTIinternalError( "Unexpected time factory" );
		}
	}

	//----------------------------------------------------------
	//                     STATIC METHODS
	//----------------------------------------------------------
}
