/* Copyright (c) 2012, Cornell University
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of Chronos nor the names of its contributors may be
 *       used to endorse or promote products derived from this software without
 *       specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/* Replicant */
#include <replicant_state_machine.h>

void*
chronosd_create(struct replicant_state_machine_context*);
void*
chronosd_recreate(struct replicant_state_machine_context* ctx,
                  const char*, size_t);
void
chronosd_destroy(struct replicant_state_machine_context* ctx, void* f);
void
chronosd_snapshot(struct replicant_state_machine_context* ctx,
                  void*, const char** data, size_t* sz);
void
chronosd_create_event(struct replicant_state_machine_context* ctx, void* obj,
                          const char* data, size_t data_sz);
void
chronosd_acquire_references(struct replicant_state_machine_context* ctx, void* obj,
                                const char* data, size_t data_sz);
void
chronosd_release_references(struct replicant_state_machine_context* ctx, void* obj,
                                const char* data, size_t data_sz);
void
chronosd_query_order(struct replicant_state_machine_context* ctx, void* obj,
                         const char* data, size_t data_sz);
void
chronosd_assign_order(struct replicant_state_machine_context* ctx, void* obj,
                          const char* data, size_t data_sz);
void
chronosd_weaver_order(struct replicant_state_machine_context* ctx, void* obj,
                          const char* data, size_t data_sz);
void
chronosd_weaver_cleanup(struct replicant_state_machine_context* ctx, void* obj,
                          const char* data, size_t data_sz);
void
chronosd_get_stats(struct replicant_state_machine_context* ctx, void* obj,
                       const char* data, size_t data_sz);

void
chronosd_new_epoch(struct replicant_state_machine_context* ctx, void* obj,
                       const char* data, size_t data_sz);

struct replicant_state_machine rsm = {
    chronosd_create,
    chronosd_recreate,
    chronosd_destroy,
    chronosd_snapshot,
    {{"create_event", chronosd_create_event},
     {"acquire_references", chronosd_acquire_references},
     {"release_references", chronosd_release_references},
     {"query_order", chronosd_query_order},
     {"assign_order", chronosd_assign_order},
     {"weaver_order", chronosd_weaver_order},
     {"weaver_cleanup", chronosd_weaver_cleanup},
     {"get_stats", chronosd_get_stats},
     {"new_epoch", chronosd_new_epoch},
     {NULL, NULL}}
};
