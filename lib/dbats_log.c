/*
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include "dbats_log.h"

int dbats_log_level = LOG_INFO;
FILE *dbats_log_file = NULL;

void dbats_log_func(int level, const char *file, int line, const char *fmt, ...)
{
    va_list va_ap;

    if (level <= dbats_log_level) {

	if (!dbats_log_file)
	    dbats_log_file = stderr;

	char msgbuf[2048];
	char datebuf[32];
	const char *prefix =
	    (level <= LOG_ERROR)    ? "ERROR: "    :
	    (level <= LOG_WARNING)  ? "WARNING: "  :
	    (level <= LOG_INFO)     ? "INFO: "     :
	    (level <= LOG_CONFIG)   ? "CONFIG: "   :
	    (level <= LOG_FINE)     ? "FINE: "     :
	    (level <= LOG_VERYFINE) ? "VERYFINE: " :
	    (level <= LOG_FINEST)   ? "FINEST: "   :
	    "";

	va_start(va_ap, fmt);
	vsnprintf(msgbuf, sizeof(msgbuf)-1, fmt, va_ap);
	msgbuf[sizeof(msgbuf)] = '\0';
	va_end(va_ap);

	time_t t = time(NULL);
	strftime(datebuf, sizeof(datebuf), "%Y-%m-%d %H:%M:%S", localtime(&t));

	fprintf(dbats_log_file, "%s %s:%d: %s%s\n",
	    datebuf, file, line, prefix, msgbuf);

	fflush(dbats_log_file);
    }
}
