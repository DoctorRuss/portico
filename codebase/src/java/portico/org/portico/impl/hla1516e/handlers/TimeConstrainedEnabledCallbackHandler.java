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
package org.portico.impl.hla1516e.handlers;

import java.util.Map;

import org.portico.lrc.services.time.msg.EnableTimeConstrained;
import org.portico.utils.messaging.MessageContext;
import org.portico.utils.messaging.MessageHandler;

import hla.rti1516e.LogicalTime;

@MessageHandler(modules="lrc1516e-callback",
                keywords="lrc1516e",
                sinks="incoming",
                priority=3,
                messages=EnableTimeConstrained.class)
public class TimeConstrainedEnabledCallbackHandler extends HLA1516eCallbackHandler
{
	//----------------------------------------------------------
	//                    STATIC VARIABLES
	//----------------------------------------------------------

	//----------------------------------------------------------
	//                   INSTANCE VARIABLES
	//----------------------------------------------------------

	//----------------------------------------------------------
	//                      CONSTRUCTORS
	//----------------------------------------------------------

	//----------------------------------------------------------
	//                    INSTANCE METHODS
	//----------------------------------------------------------
	public void initialize( Map<String,Object> properties )
	{
		super.initialize( properties );
	}
	
	public void process( MessageContext context ) throws Exception
	{
		LogicalTime currentTime = helper.getCurrentLogicalTime();
		logger.trace( "CALLBACK timeConstrainedEnabled(time="+currentTime+")" );
		fedamb().timeConstrainedEnabled( currentTime );
		logger.trace( "         timeConstrainedEnabled() callback complete" );
		context.success();
	}
	
	//----------------------------------------------------------
	//                     STATIC METHODS
	//----------------------------------------------------------
}
