/*
 *   Copyright 2015 The Portico Project
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
package org.portico.bindings.jgroups.channel;

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;
import org.jgroups.Address;
import org.jgroups.MembershipListener;
import org.jgroups.Message;
import org.jgroups.MessageListener;
import org.jgroups.View;
import org.jgroups.Message.Flag;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.util.Util;
import org.portico.bindings.jgroups.channel.ControlHeader;
import org.portico.lrc.compat.JResignAction;
import org.portico.lrc.model.ObjectModel;
import org.portico.lrc.services.federation.msg.ResignFederation;
import org.portico.lrc.utils.MessageHelpers;

/**
 * This class implements the various JGroups listener interfaces that allow it to receive
 * notifications from a channel when channel membership changes, state is required or messages
 * are ready for processing.
 * <p/>
 * Instances of this class should be contained inside a {@link FederationChannel}. Incoming
 * message are handed off to the message receiver that sits inside the FederationChannel (which
 * in turn routes the messages to the appropriate LRC or /dev/null if we're not connected).
 */
public class ChannelListener implements RequestHandler, MessageListener, MembershipListener
{
	//----------------------------------------------------------
	//                    STATIC VARIABLES
	//----------------------------------------------------------

	//----------------------------------------------------------
	//                   INSTANCE VARIABLES
	//----------------------------------------------------------
	private Logger logger;
	private String channelName;
	private Channel channel;

	// We store the current view so that we can assess changes to cluster
	// membership and if necessary, issue some delete notifications for
	// federates that have dropped off.
	private View currentView;

	//----------------------------------------------------------
	//                      CONSTRUCTORS
	//----------------------------------------------------------
	protected ChannelListener( Channel channel )
	{
		this.channel = channel;
		this.logger = channel.logger;
		this.channelName = channel.getChannelName();
		this.currentView = null;
	}

	//----------------------------------------------------------
	//                    INSTANCE METHODS
	//----------------------------------------------------------

	/////////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////// MembershipListener Methods ////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////////
	/**
	 * @param newView The new channel membership
	 */
	public void viewAccepted( View newView )
	{
		if( currentView == null )
		{
			currentView = newView;
			return;
		}

		// Find out if the local coordinator left. If so, we may have to step in
		Address coordinator = channel.manifest.getCoordinator();
		if( newView.containsMember(coordinator) == false )
		{
			// they're gone!? :(
		}

		//FIXME Check to see if anyone has disappeared
		logger.debug( "View accepted: "+newView );
	}

	/**
	 * A hint from JGroups that this federate may have gone AWOL.
	 */
	public void suspect( Address suspectedDropout )
	{
		// just log for information
		channel.manifest.getFederateName( suspectedDropout );
		logger.warn( "Detected that federate ["+1+"] may have crashed, investigating..." );
	}

	/**
	 * The block/unblock messages are called when a FLUSH is invoked. A FLUSH will prevent
	 * channel members from sending new messages until all the existing messages that they
	 * have sent have been delivered to all participants. Typically this is done when a new
	 * connection to the channel is made, to ensure that all exiting messages are sent to
	 * the same set of group members that were present when the message was sent (not the
	 * updated set modified by the joining of a new member).
	 * <p/>
	 * The implementation of the FLUSH protocol will block when a flush is invoked, so
	 * we don't have to do anything special. This is a no-op for us. 
	 */
	public void block()
	{
		// ignore
	}

	/**
	 * @see #block()
	 */
	public void unblock()
	{
		// ignore
	}

	/////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////// RequestHandler Methods ///////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////////
	/**
	 * Synchronous message received
	 * <p/>
	 * This method is called by JGroups when we have been provided with a synchronous message
	 * against which we are expected to supply a response. We pretty much just hand it off to
	 * the channel's MessageReceiver.
	 */
	public Object handle( Message message )
	{
		// log that we have an incoming message
		if( logger.isTraceEnabled() )
		{
			logger.trace( "(incoming) synchronous, channel="+channelName+", size="+
			              message.getLength()+", source="+message.getSrc() );
		}

		return channel.receiver.receiveSynchronous( message );
	}

	/////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////// MessageListener Methods //////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////////
	/** No-op. */ public void getState( OutputStream stream ) {}
	/** No-op. */ public void setState( InputStream stream ) {}

	/**
	 * Asynchronous message received
	 * <p/>
	 * Called when JGroups has received an asynchronous method for us to process.
	 */
	public void receive( Message message )
	{
		// log that we have an incoming message
		if( logger.isTraceEnabled() )
		{
			logger.trace( "(incoming) asynchronous, channel="+channelName+", size="+
			              message.getLength()+", source="+message.getSrc() );
		}

		ControlHeader header = getControlHeader( message );
		if( header == null )
		{
			// just a regular message, hand it off to our receiver
			channel.receiver.receiveAsynchronous( message );
		}
		else
		{
			switch( header.getMessageType() )
			{
				case ControlHeader.FIND_COORDINATOR:
					logger.debug( "(GMS) findCoordinator("+message.getSrc()+")" );
					incomingFindCoordinator( message );
					break;
				case ControlHeader.SET_MANIFEST:
					logger.debug( "(GMS) setManifest("+message.getSrc()+")" );
					incomingSetManifest( message );
					break;
				case ControlHeader.CREATE_FEDERATION:
					logger.debug( "(GMS) createFederation("+message.getSrc()+")" );
					incomingCreateFederation( message );
					break;
				case ControlHeader.JOIN_FEDERATION:
					logger.debug( "(GMS) joinFederation("+message.getSrc()+")" );
					incomingJoinFederation( message );
					break;
				case ControlHeader.RESIGN_FEDERATION:
					logger.debug( "(GMS) resignFederation("+message.getSrc()+")" );
					incomingResignFederation( message );
					break;
				case ControlHeader.DESTROY_FEDERATION:
					logger.debug( "(GMS) destroyFederation("+message.getSrc()+")" );
					incomingDestroyFederation( message );
					break;
				case ControlHeader.GOODBYE:
					logger.debug( "(GMS) goodbye("+message.getSrc()+")" );
					incomingGoodbye( message );
					break;
				default:
					logger.warn( "Unknown control message [type="+header.getMessageType()+"]. Ignore." );
			}
		}
	}

	////////////////////////////////////////////////////////////
	///////////////// Message Handling Helpers /////////////////
	////////////////////////////////////////////////////////////

	private final ControlHeader getControlHeader( Message message )
	{
		return (ControlHeader)message.getHeader( ControlHeader.HEADER );
	}

	/**
	 * Someone has joined the channel and is trying to find out who the current coordinator
	 * is (construct distinct from the underlying JGroups coordinator installed by the GMS).
	 * 
	 * If we are the coordinator, assign the member a handle (done once when they join a 
	 * channel so that there are no fights over handle assignments), update our local manifest
	 * with that information then serialize the updated manifest and send it out. 
	 */
	private void incomingFindCoordinator( Message message )
	{
		// disregard if this is our own
		Address from = message.getSrc();
		if( from.equals(channel.jchannel.getAddress()) )
			return;
		
		if( channel.manifest.isCoordinator() )
		{
			logger.debug( "Received request for manifest from "+from );
			
			// tell the manifest a member has connected - this is where the handle is assigned
			channel.manifest.memberConnectedToChannel( message.getSrc() );
			
			Message response = new Message();
			response.putHeader( ControlHeader.HEADER, ControlHeader.setManifest() );
			response.setFlag( Flag.DONT_BUNDLE );
			response.setFlag( Flag.NO_FC );
			response.setFlag( Flag.OOB );
//FIXME			response.setFlag( Flag.RSVP );        // don't do jack until we know they have it
			response.setDest( message.getSrc() ); // send only to the requestor

			try
			{
				byte[] buffer = Util.objectToByteBuffer( channel.manifest );
				response.setBuffer( buffer );
				channel.jchannel.send( response );
				
				logger.debug( "Sent manifest ("+buffer.length+"b) to "+from );
			}
			catch( Exception e )
			{
				logger.error( "Error while sending manifest to ["+from+"]: "+e.getMessage(), e );
			}
		}
	}
	
	/**
	 * Channel co-ordinator has sent us a copy of the current manifest. Install it as our
	 * own, caring lovingly for it only to have it turn into a horrible teen monster before
	 * we have to disconnect and get a beer.
	 */
	private void incomingSetManifest( Message message )
	{
		// ignore if we are the coordinator - we will have created this
		if( channel.manifest != null && channel.manifest.isCoordinator() )
			return;
		
		logger.debug( "Received updated manifest from "+message.getSrc() );
		
		try
		{
			Manifest manifest = (Manifest)Util.objectFromByteBuffer( message.getBuffer() );
			manifest.setLocalAddress( channel.jchannel.getAddress() );
			channel.manifest = manifest;
			logger.debug( "Installed new manifest (follows)" );
			logger.debug( manifest );
		}
		catch( Exception e )
		{
			logger.error( "Error installing new manifest: "+e.getMessage(), e );
		}
	}

	/**
	 * This method is invoked when the channel has received a federation creation message.
	 * It goes through the process of storing the object model inside the manifest and marking
	 * the federation as created.
	 */
	private synchronized void incomingCreateFederation( Message message )
	{
		byte[] buffer = message.getBuffer();
		logger.debug( "Received federation creation notification: federation="+channelName+
		              ", fomSize="+buffer.length+"b, source=" + message.getSrc() );
		
		try
		{
			// turn the buffer into a FOM and store it on the federation manifest
			channel.manifest.federationCreated( (ObjectModel)Util.objectFromByteBuffer(buffer) );
			logger.info( "Federation ["+channelName+"] has been created" );
		}
		catch( Exception e )
		{
			logger.error( "Error installing FOM for federation ["+channelName+"]: "+
			              "this federate will not be able to join the federation", e );
		}
	}

	/**
	 * When an existing channel member joins the federation, they send out a join notice.
	 * We watch for these and record the change in our manifest when that happens. We know
	 * the handle they will use (as they were assigned a number by the coordinator when
	 * they connected), so there is no need to get this information from the notification.
	 */
	private void incomingJoinFederation( Message message )
	{
		String federateName = new String( message.getBuffer() );
		logger.debug( "Received federate join notification: federate="+federateName+
		              ", federation="+channelName+", source="+message.getSrc() );
		
		channel.manifest.federateJoined( message.getSrc(), federateName );
		logger.info( "Federate ["+federateName+"] joined federation ["+channelName+"]" );
	}

	/**
	 * This method is called when a federate is resigning from a federation. We catch the message
	 * and hand it off to the receiver to process, and then we go through the steps of removing
	 * the connection as a joined federate inside the manifest. The connection is still a member
	 * of the channel, just no longer a joined federate.
	 */
	private void incomingResignFederation( Message message )
	{
		// queue the message for processing
		channel.receiver.receiveAsynchronous( message );

		// get the federate name
		String federateName = channel.manifest.getFederateName( message.getSrc() );
		
		// record the resignation in the roster
		logger.trace( "Received federate resign notification: federate="+federateName+
			          ", federation="+channelName+", source="+message.getSrc() );
			
		channel.manifest.federateResigned( message.getSrc() );
		logger.info( "Federate ["+federateName+"] has resigned from ["+channelName+"]" );
	}

	/**
	 * Received a remote federation destroy notification. Check to make sure we're in a state
	 * where we can do this (we have an active federation but it doesn't contain any joined
	 * federates). If we are not in a suitable state we log an error and ignore the request,
	 * otherwise we remove the FOM associated with the channel and flick the created status
	 * to false.
	 */
	private void incomingDestroyFederation( Message message )
	{
		String federationName = new String( message.getBuffer() );
		Address address = message.getSrc();

		logger.trace( "Received federate destroy notification: federation="+federationName+
		              ", source=" + address );
		
		if( channel.manifest.containsFederation() == false )
		{
			logger.error( "Connection ["+address+"] apparently destroyed federation ["+
			              federationName+"], but we didn't know it existed, ignoring..." );
		}
		else if( channel.manifest.getFederateHandles().size() > 0 )
		{
			logger.error( "Connection ["+address+"] apparnetly destoryed federation ["+
			              federationName+"], but we still have active federates, ignoring... " );
		}
		else
		{
			channel.manifest.federationDestroyed();
			logger.info( "Federation ["+federationName+"] has been destroyed" );
		}
	}

	/**
	 * A member of the channel has disconnected. Run any cleanup:
	 * 
	 *   - If they are listed as a joined federate, fake a resign message
	 *   - If they were the coordinator, figure out who the new supreme ruler is
	 */
	private void incomingGoodbye( Message message )
	{
		Address leaver = message.getSrc();
		logger.trace( "Received goodbye notification: channel="+channelName+", from="+leaver );

		//
		// was this federate joined (or just a member)?
		//
		if( channel.manifest.isJoinedFederate(leaver) )
		{
			int federateHandle = channel.manifest.getFederateHandle( leaver );
			String federateName = channel.manifest.getFederateName( leaver );
			
			// synthesize a resign notification
			ResignFederation resign =
				new ResignFederation( JResignAction.DELETE_OBJECTS_AND_RELEASE_ATTRIBUTES );
			resign.setSourceFederate( federateHandle );
			resign.setFederateName( federateName );
			resign.setFederationName( channelName );
			resign.setImmediateProcessingFlag( true );
			
			Message resignMessage = new Message( channel.jchannel.getAddress(),
			                                     channel.jchannel.getAddress(),
			                                     MessageHelpers.deflate(resign) );
			
			// dispatch resign message for the crashed federate to the LRC 
			this.receive( resignMessage );
			
			logger.info( "Federate ["+federateName+","+federateHandle+
			             "] disconnected, synthesizing resign message" );
		}

		//
		// Remove the leaver from the manifest. This will also update
		// our idea about who the coordinator is if that is necessary.
		//
		channel.manifest.memberLeftChannel( leaver );
	}
	
	//----------------------------------------------------------
	//                     STATIC METHODS
	//----------------------------------------------------------
}
