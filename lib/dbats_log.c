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
#include <sys/types.h>
#include <unistd.h> // getpid()
#include "dbats_log.h"

int dbats_log_level = DBATS_LOG_INFO;
FILE *dbats_log_file = NULL;
void (*dbats_log_callback)(int level, const char *file, int line, const char *msg) = NULL;

void dbats_log_func(int level, const char *file, int line, const char *fmt, ...)
{
    va_list va_ap;

    if (level <= dbats_log_level) {

	char msgbuf[2048];
	va_start(va_ap, fmt);
	vsnprintf(msgbuf, sizeof(msgbuf)-1, fmt, va_ap);
	msgbuf[sizeof(msgbuf)-1] = '\0';
	va_end(va_ap);

	if (dbats_log_callback) {
	    dbats_log_callback(level, file, line, msgbuf);

	} else {
	    if (!dbats_log_file)
		dbats_log_file = stderr;

	    char datebuf[32];
	    time_t t = time(NULL);
	    strftime(datebuf, sizeof(datebuf), "%Y-%m-%d %H:%M:%S", localtime(&t));

	    const char *prefix =
		(level <= DBATS_LOG_ERR)    ? "ERROR: "    :
		(level <= DBATS_LOG_WARN)   ? "WARNING: "  :
		(level <= DBATS_LOG_INFO)   ? "INFO: "     :
		(level <= DBATS_LOG_CONFIG) ? "CONFIG: "   :
		(level <= DBATS_LOG_FINE)   ? "FINE: "     :
		(level <= DBATS_LOG_VFINE)  ? "VERYFINE: " :
		(level <= DBATS_LOG_FINEST) ? "FINEST: "   :
		"";

	    fprintf(dbats_log_file, "%s %u: %s:%d: %s%s\n",
		datebuf, getpid(), file, line, prefix, msgbuf);

	    fflush(dbats_log_file);
	}
    }
}
