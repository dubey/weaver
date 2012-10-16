/*
 * =====================================================================================
 *
 *       Filename:  property.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  Friday 12 October 2012 01:28:02  EDT
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Ayush Dubey (), dubey@cs.cornell.edu
 *   Organization:  Cornell University
 *
 * =====================================================================================
 */

namespace db 
{
namespace element
{
	class property
	{
		public:
			property (char *_key, char *_value);
		
		public:
			char *key;
			char *value;
	}
}
}
