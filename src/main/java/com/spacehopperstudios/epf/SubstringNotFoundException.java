//  
//  SubstringNotFoundException.java
//  epfimporter
//
//  Created by William Shakour on October 4, 2015.
//  Copyright © 2015 WillShex Limited. All rights reserved.
//
package com.spacehopperstudios.epf;

/**
 * @author William Shakour (billy1380)
 *
 */
public class SubstringNotFoundException extends Exception {

	private static final long serialVersionUID = 1L;

	public SubstringNotFoundException(String expl) {
		super(expl);
	}

}
