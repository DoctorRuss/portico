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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.log4j.Logger;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Message.Flag;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.Util;
import org.portico.bindings.jgroups.Auditor;
import org.portico.bindings.jgroups.Configuration;
import org.portico.bindings.jgroups.MessageReceiver;
import org.portico.lrc.LRC;
import org.portico.lrc.PorticoConstants;
import org.portico.lrc.compat.JFederateAlreadyExecutionMember;
import org.portico.lrc.compat.JFederatesCurrentlyJoined;
import org.portico.lrc.compat.JFederationExecutionAlreadyExists;
import org.portico.lrc.compat.JFederationExecutionDoesNotExist;
import org.portico.lrc.compat.JRTIinternalError;
import org.portico.lrc.model.ObjectModel;
import org.portico.lrc.utils.MessageHelpers;
import org.portico.utils.messaging.PorticoMessage;

/**
 * This class represents a channel devoted to supporting an active Portico Federation.
 * Convenience methods are provided here to manage members joining and resigning from
 * a federation and the sending of messages within the group.
 */
public class Channel
{
	//----------------------------------------------------------
	//                    STATIC VARIABLES
	//----------------------------------------------------------
	static
	{
		// we need this to get around a problem with JGroups and IPv6 on a Linux/Java 5 combo
		System.setProperty( "java.net.preferIPv4Stack", "true" );
	}

	//----------------------------------------------------------
	//                   INSTANCE VARIABLES
	//----------------------------------------------------------
	protected Logger logger;
	private String channelName;

	// write metadata about incoming/outgoing message flow
	private Auditor auditor;

	// JGroups connection information
	protected boolean connected;
	protected JChannel jchannel;
	private MessageDispatcher jdispatcher;
	private ChannelListener jlistener;

	// gateway to pass received messages back
	protected MessageReceiver receiver;

	// federation and shared state
	protected Manifest manifest;

	//----------------------------------------------------------
	//                      CONSTRUCTORS
	//----------------------------------------------------------
	public Channel( String name )
	{
		this.channelName = name;
		this.logger = Logger.getLogger( "portico.lrc.jgroups" );
				
		// create this, but leave as disabled for now - gets turned on in joinFederation
		this.auditor = new Auditor();
		
		// channel details set when we connect
		this.connected = false;
		this.jchannel = null;
		this.jdispatcher = null;
		this.jlistener = new ChannelListener( this );
		
		this.receiver = new MessageReceiver( this.auditor );

		// the manifest is created in the viewAccepted ??? method which is called
		// as soon as we connect to a channel
		this.manifest = null; 
	}


	//----------------------------------------------------------
	//                    INSTANCE METHODS
	//----------------------------------------------------------

	//////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////// Channel Lifecycle Methods /////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////
	/** Close off the connection to the existing JGroups channel */
	public void disconnect()
	{
		// send a goodbye notification
		try
		{
			Message message = new Message();
			message.putHeader( ControlHeader.HEADER, ControlHeader.goodbye() );
			message.setFlag( Flag.DONT_BUNDLE );
			message.setFlag( Flag.NO_FC );
			message.setFlag( Flag.OOB );
			this.jchannel.send( message );
		}
		catch( Exception e )
		{
			logger.error( "Exception while sending channel goodbye message: "+e.getMessage(), e );
		}

		// Disconnect
		this.jchannel.disconnect();
		this.jchannel.close();
		this.connected = false;
		logger.debug( "Connection closed to channel ["+channelName+"]" );
	}

	/**
	 * Create the underlying JGroups channel and connect to it. If we have WAN support
	 * enabled then we will create a slightly different protocol stack based on the
	 * configuration. Should we be unable to create or connect to the channel, a generic
	 * internal error exception will be thrown.
	 */
	public void connect() throws JRTIinternalError
	{
		// connect to the channel
		if( this.isConnected() )
			return;
		
		try
		{
			logger.trace( "ATTEMPT Connecting to channel ["+channelName+"]" );

			// set the channel up
			this.jchannel = constructChannel();
			this.jdispatcher = new MessageDispatcher( jchannel, jlistener, jlistener, jlistener );

			// connects to the channel and fetches state in single action
			this.jchannel.connect( channelName );
			
			// find the coordinator
			this.findCoordinator();
			
			// all done
			this.connected = true;
			logger.debug( "SUCCESS Connected to channel ["+channelName+"]" );
		}
		catch( Exception e )
		{
			logger.error( "ERROR Failed to connect to channel ["+channelName+"]: "+
			              e.getMessage(), e );
			throw new JRTIinternalError( e.getMessage(), e );
		}
	}
	
	/**
	 * This method constructs the channel, including any nitty-gritty details (such as thread pool
	 * details or the like).
	 */
	private JChannel constructChannel() throws Exception
	{
		// create a different channel depending on whether we are trying to use the WAN
		// or local network infrastructure
		JChannel channel = null;
		if( Configuration.isWanEnabled() )
		{
			logger.info( "WAN mode has been enabled" );
			writeRelayConfiguration();
			channel = new JChannel( "etc/jgroups-relay.xml" );
		}
		else
		{
			channel = new JChannel( "etc/jgroups-udp.xml" );
		}

		// if we're not using daemon threads, return without resetting the thread groups
		if( Configuration.useDaemonThreads() == false )
			return channel;

		// we are using daemon threds, so let's set the channel up to do so
		// set the thread factory on the transport
		ThreadGroup threadGroup = channel.getProtocolStack().getTransport().getChannelThreadGroup();
		DefaultThreadFactory factory = new DefaultThreadFactory( threadGroup, "Incoming", true );
		channel.getProtocolStack().getTransport().setThreadFactory( factory );
		channel.getProtocolStack().getTransport().setOOBThreadPoolThreadFactory( factory );
		channel.getProtocolStack().getTransport().setTimerThreadFactory( factory );

		// set the thread pools on the transport
		ThreadPoolExecutor regular =
		    (ThreadPoolExecutor)channel.getProtocolStack().getTransport().getDefaultThreadPool();
		regular.setThreadFactory( new DefaultThreadFactory(threadGroup,"Regular",true) );

		// do the same for the oob pool
		ThreadPoolExecutor oob =
		    (ThreadPoolExecutor)channel.getProtocolStack().getTransport().getOOBThreadPool();
		oob.setThreadFactory( new DefaultThreadFactory(threadGroup,"OOB",true) );

		return channel;
	}

	/**
	 * The RELAY2 configuration requires a bunch of information from us and will currently
	 * only suck it in from an XML configuration file... (╯°□°）╯︵ ┻━┻)
	 * 
	 * This method will write an appropriate relay configuration file based on the given
	 * RID information. It will be written into a place that our jgroups configuration will
	 * find it.
	 */
	private void writeRelayConfiguration() throws Exception
	{
		// write the header
		StringBuilder builder = new StringBuilder();
		builder.append( "<RelayConfiguration xmlns=\"urn:jgroups:relay:1.0\"><sites>" );

		// write an entry for each of the remotes listed in the RID file
		int counter = 0;
		String bridgeConfig = "etc/jgroups-tunnel.xml";
		for( String remote : Configuration.getWanRemotes() )
		{
			builder.append( "<site name=\""+remote+"\" id=\""+counter+
			                "\"><bridges><bridge config=\""+bridgeConfig+
			                "\" name=\"backbone\"/></bridges></site>" );
			counter++;
		}

		// write the tail
		builder.append( "</sites></RelayConfiguration>" );

		// write the file out
		String tempfile = System.getProperty("java.io.tmpdir")+File.separator+"portico-relay.xml";
		Files.write( Paths.get(tempfile),
		             builder.toString().getBytes(),
		             StandardOpenOption.CREATE,
		             StandardOpenOption.WRITE,
		             StandardOpenOption.TRUNCATE_EXISTING );
	}

	/**
	 * Finds the channel co-ordinator and gets the manifest from them. If there is no
	 * co-ordinator, step up and be one!
	 */
	private void findCoordinator() throws Exception
	{
		//////////////////////////////
		// Ask for the Co-ordinator //
		//////////////////////////////
		// announce that we have joined
		Message message = new Message();
		message.putHeader( ControlHeader.HEADER, ControlHeader.findCoordinator() );
		message.setFlag( Flag.DONT_BUNDLE );
		message.setFlag( Flag.NO_FC );
		message.setFlag( Flag.OOB );

		// send the message out - because this is marked with the RSVP flag the send
		// call will block until all other channel participants have received the
		// message and acknowledged it
		this.jchannel.send( message );
		
		// sleep
		PorticoConstants.sleep( 2000 ); // FIXME replace with configurable
		
		// got mail?
		if( manifest == null )
		{
			logger.info( "No co-ordinator found - appointing myself!" );
			this.manifest = new Manifest( channelName, jchannel.getAddress() );
			this.manifest.setCoordinator( jchannel.getAddress() );
		}
	}

	//////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////// General Sending Methods ///////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////
	public Manifest getManifest() { return manifest; }
	public String getChannelName() { return this.channelName; }
	public boolean isConnected() { return this.connected; }

	/**
	 * This method will send the provided message to all federates in the federation. If there
	 * is a problem serializing or sending the message, a {@link JRTIinternalError} is thrown.
	 * <p/>
	 * No special flags are set on these messages. They are asynchronous, subject to flow control
	 * and bundling.
	 * 
	 * @param payload The message to be sent.
	 * @throws JRTIinternalError If there is a problem serializing or sending the message
	 */
	public void send( PorticoMessage payload ) throws JRTIinternalError
	{
		// turn the packet into a message
		byte[] data = MessageHelpers.deflate( payload );
		Message message = new Message( null /*destination*/, null /*source*/, data );
		message.setBuffer( data );

		// Log an audit message for the send
		if( auditor.isRecording() )
			auditor.sent( payload, data.length );
		
		// write the message
		if( logger.isTraceEnabled() )
		{
			logger.trace( "(outgoing) payload="+payload.getClass().getSimpleName()+", size="+
			              data.length+", channel="+channelName );
		}
		
		try
		{
			jchannel.send( message );
		}
		catch( Exception e )
		{
			throw new JRTIinternalError( "Problem sending message: channel="+channelName+
			                             ", error message="+e.getMessage(), e );
		}
	}
	
	//////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////// Federation Lifecycle Methods ////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////
	public void createFederation( ObjectModel fom ) throws Exception
	{
		logger.debug( "REQUEST createFederation: name=" + channelName );

		// make sure we're not already connected to an active federation
		if( manifest.containsFederation() )
		{
			logger.error( "FAILURE createFederation: already exists, name="+channelName );
			throw new JFederationExecutionAlreadyExists( "federation exists: "+channelName );
		}
		
		// send out create federation call and get an ack from everyone
		Message message = new Message();
		message.putHeader( ControlHeader.HEADER, ControlHeader.newCreateHeader() );
		message.setBuffer( Util.objectToByteBuffer(fom) );
		message.setFlag( Flag.DONT_BUNDLE );
		message.setFlag( Flag.NO_FC );
		message.setFlag( Flag.OOB );
		message.setFlag( Flag.RSVP );

		// send the message out - because this is marked with the RSVP flag the send
		// call will block until all other channel participants have received the
		// message and acknowledged it
		this.jchannel.send( message );

		logger.info( "SUCCESS createFederation: name=" + channelName );
	}
	
	public String joinFederation( String federateName, LRC lrc ) throws Exception
	{
		logger.debug( "REQUEST joinFederation: federate="+federateName+", federation="+channelName );

		// check to see if there is an active federation
		if( manifest.containsFederation() == false )
		{
			logger.info( "FAILURE joinFederation: federation doesn't exist, name="+channelName );
			throw new JFederationExecutionDoesNotExist( "federation doesn't exist: "+channelName );
		}

		// check to see if someone already exists with the same name
		// If we have been configured to allow non-unique names, check, but don't error,
		// just augment the name from "federateName" to "federateName (handle)"
		if( manifest.containsFederate(federateName) )
		{
			if( PorticoConstants.isUniqueFederateNamesRequired() )
			{
				logger.info( "FAILURE joinFederation: federate="+federateName+", federation="+
				             channelName+": Federate name in use" );
				throw new JFederateAlreadyExecutionMember( "federate name in use: "+federateName );
			}
			else
			{
				federateName += " ("+manifest.getLocalFederateHandle()+")";
				logger.warn( "WARNING joinFederation: name in use, changed to "+federateName );
			}
		}

		// Enable the auditor if we are configured to use it
		if( Configuration.isAuditorEnabled() )
			this.auditor.startAuditing( channelName, federateName, lrc );
		
		// link up the message receiver to the LRC we're joined to so that
		// messages can start flowing right away
		this.receiver.linkToLRC( lrc ); // FIXME make sure the receiver takes notice
	
		// send the notification to all other members and get an ack from everyone
		Message message = new Message();
		message.putHeader( ControlHeader.HEADER, ControlHeader.newJoinHeader() );
		message.setBuffer( federateName.getBytes() );
		message.setFlag( Flag.DONT_BUNDLE );
		message.setFlag( Flag.NO_FC );
		message.setFlag( Flag.OOB );
		message.setFlag( Flag.RSVP );

		// send the message out, blocks until all acknowledge its receipt
		this.jchannel.send( message );
		
		logger.info( "SUCCESS Joined federation with name="+federateName );
		return federateName;		
	}
	
	/**
	 * Sends the resignation notification out with the appropriate header so that it is
	 * detected by other channel members, allowing them to update their manifest appropriately.
	 * If there is a problem sending the messaage, an exception is thrown.
	 */
	public void resignFederation( PorticoMessage resignMessage ) throws Exception
	{
		// get the federate name before we send and cause it to be removed from the manifest
		String federateName = this.manifest.getLocalFederateName();
		logger.debug( "REQUEST resignFederation: federate="+federateName+
		              ", federation="+channelName );
		
		// send the notification out to all receivers and wait for acknowledgement from each
		Message message = new Message();
		message.putHeader( ControlHeader.HEADER, ControlHeader.newResignHeader() );
		message.setBuffer( MessageHelpers.deflate(resignMessage) );
		message.setFlag( Flag.DONT_BUNDLE );
		message.setFlag( Flag.NO_FC );
		message.setFlag( Flag.OOB );
		message.setFlag( Flag.RSVP );
		this.jchannel.send( message );
		
		// all done, disconnect our incoming receiver
		// we received the message sent above as well, so the receiver will update the
		// manifest as it updates it for any federate resignation
		logger.info( "SUCCESS Federate ["+federateName+"] resigned from ["+channelName+"]" );
		this.receiver.unlink();
		this.auditor.stopAuditing();
	}
	
	/**
	 * Validates that the federation is in a destroyable state (active federation but no
	 * joined federates) and then sends the destroy request to all other connected members,
	 * blocking until they acknowledge it.
	 * 
	 * @throws Exception If there is no federation active or there are federates still joined to
	 *                   it, or if there is a problem flushing the channel before we send.
	 */
	public void destroyFederation() throws Exception
	{
		logger.debug( "REQUEST destroyFederation: name=" + channelName );
		
		// check to make sure there is a federation to destroy
		if( manifest.containsFederation() == false )
		{
			logger.error( "FAILURE destoryFederation ["+channelName+"]: doesn't exist" );
			throw new JFederationExecutionDoesNotExist( "doesn't exist: "+channelName );
		}

		// check to make sure there are no federates still joined
		if( manifest.getFederateHandles().size() > 0 )
		{
			String stillJoined = manifest.getFederateHandles().toString();
			logger.info( "FAILURE destroyFederation ["+channelName+"]: federates still joined "+
			             stillJoined );
			throw new JFederatesCurrentlyJoined( "federates still joined: "+stillJoined );
		}

		// send the notification out to all receivers and wait for acknowledgement from each
		Message message = new Message();
		message.putHeader( ControlHeader.HEADER, ControlHeader.newDestroyHeader() );
		message.setBuffer( channelName.getBytes() );
		message.setFlag( Flag.DONT_BUNDLE );
		message.setFlag( Flag.NO_FC );
		message.setFlag( Flag.OOB );
		message.setFlag( Flag.RSVP );
		this.jchannel.send( message );
		
		// we don't need to update the manifest directly here, that will be done by
		// out message listener, which will have received the message we sent above

		logger.info( "SUCCESS destroyFederation: name=" + channelName );
	}
	
	//----------------------------------------------------------
	//                     STATIC METHODS
	//----------------------------------------------------------
}
